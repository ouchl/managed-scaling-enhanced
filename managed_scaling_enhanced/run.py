from datetime import datetime, timedelta, timezone
import boto3
from orjson import orjson
from dateutil import parser

from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import EMREvent, Event
import logging
from managed_scaling_enhanced.scale import resize_cluster
from managed_scaling_enhanced.metrics import *

logger = logging.getLogger(__name__)
emr_client = boto3.client('emr', config=boto3_config)
ec2_client = boto3.client('ec2', config=boto3_config)
sqs = boto3.client('sqs')


def get_latest_ready_time(instances):
    latest_ready_time = datetime.min
    for instance in instances:
        ready_time = instance['Status']['Timeline']['ReadyDateTime']
        ready_time = ready_time.astimezone(timezone.utc).replace(tzinfo=None)
        latest_ready_time = max(latest_ready_time, ready_time)
    return latest_ready_time


def do_run(cluster: Cluster, dry_run, session):
    response = emr_client.describe_cluster(ClusterId=cluster.id)
    if response['Cluster']['Status']['State'] not in ('RUNNING', 'WAITING'):
        logger.info(f'Skipping cluster {cluster.id} because it is not running.')
        return
    cluster.cluster_name = response['Cluster']['Name']
    master_public_dns = response['Cluster']['MasterPublicDnsName']
    cluster.master_dns_name = master_public_dns
    cluster.current_managed_scaling_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster.id)['ManagedScalingPolicy']
    if cluster.is_fleet:
        cluster.instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
    else:
        cluster.instance_groups = emr_client.list_instance_groups(ClusterId=cluster.id)['InstanceGroups']
    metric = collect_metrics(cluster)
    logger.info(f'Collected metrics: {metric.__dict__}')
    session.add(metric)
    session.commit()
    # Update instances cpu time
    cpu_utilization = get_cpu_utilization(cluster, session)
    if cpu_utilization is None:
        logger.info(f'Skipping cluster {cluster.id} no cpu utilization is found.')
        return
    lb_metrics = get_lookback_metrics(cluster, session)
    if len(lb_metrics) < 2:
        logger.info(f'Skipping cluster {cluster.id} because there are not enough metrics.')
        return
    avg_metric = collect_avg_metrics(cluster, lb_metrics)
    avg_metric.cpu_utilization = cpu_utilization
    session.add(avg_metric)
    session.commit()
    resize_cluster(cluster, session, dry_run)


def clean(session):
    retention_days = 2
    session.query(Metric).filter(Metric.event_time < (datetime.utcnow() - timedelta(days=retention_days))).delete(synchronize_session=False)
    session.query(AvgMetric).filter(AvgMetric.event_time < (datetime.utcnow() - timedelta(days=retention_days))).delete(synchronize_session=False)
    session.query(Event).filter(Event.event_time < (datetime.utcnow() - timedelta(days=retention_days))).delete(
        synchronize_session=False)
    session.query(EMREvent).filter(EMREvent.event_time < (datetime.utcnow() - timedelta(days=retention_days))).delete(
        synchronize_session=False)
    session.query(CpuUsage).filter(CpuUsage.event_time < (datetime.utcnow() - timedelta(days=1))).delete(
        synchronize_session=False)


def read_sqs(name):
    queue_url = sqs.get_queue_url(QueueName=name)['QueueUrl']
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=30,
        WaitTimeSeconds=2
    )
    messages = response.get('Messages', [])
    with Session() as session:
        for message in messages:
            logger.debug(f'Received EMR event message: {message}')
            body = orjson.loads(message['Body'])
            event_type = body.get('detail-type')
            event_time = parser.parse(body['time']).replace(tzinfo=None)
            session.add(EMREvent(message=body.get('detail').get('message'), event_type=event_type,
                                 cluster_id=body.get('detail').get('clusterId'),
                                 state=body.get('detail').get('state'),
                                 source=body.get('source'),
                                 raw_message=body,
                                 event_time=event_time, create_time=datetime.utcnow()))
            session.commit()
            # Assuming processing of the message is successful, delete the message
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])


def run(dry_run, event_queue):
    session = Session()
    clusters = session.query(Cluster).all()
    cluster_ids = [cluster.id for cluster in clusters]
    session.close()
    for cluster_id in cluster_ids:
        try:
            with Session() as session:
                logger.info(f'####################################### Start {cluster_id} ##########################################')
                cluster = session.get(Cluster, cluster_id)
                if not cluster.active:
                    logger.info(f'Skipping cluster {cluster_id} because it is not active.')
                    continue
                do_run(cluster, dry_run, session)
                session.commit()
                logger.info(f'####################################### End {cluster_id} ##########################################\n\n\n')
        except Exception as e:
            logger.exception(f'Cluster {cluster_id} error: {e}')
    if event_queue:
        read_sqs(event_queue)
    with Session() as session:
        # clean table
        clean(session)
        session.commit()


if __name__ == '__main__':
    run(dry_run=True, event_queue=None)
