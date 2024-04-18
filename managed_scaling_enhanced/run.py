import os
from datetime import datetime, timedelta, timezone
import boto3
from orjson import orjson
from dateutil import parser

from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster, CpuUsage, EMREvent
import logging
import requests
from managed_scaling_enhanced.scale import resize_cluster
from dataclasses import dataclass


logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')
ec2_client = boto3.client('ec2')
sqs = boto3.client('sqs')


@dataclass
class Instance:
    instance_id: str
    host_name: str


def get_current_yarn_metrics(dns_name):
    response = requests.get(f"http://{dns_name}:8088/ws/v1/cluster/metrics", timeout=5)
    response.raise_for_status()
    return {k: v for k, v in response.json().get('clusterMetrics', {}).items() if not k.endswith('AcrossPartition')}


def get_instances_native(cluster: Cluster):
    instances = []
    paginator = emr_client.get_paginator('list_instances')
    response_iterator = paginator.paginate(
        ClusterId=cluster.id,
        InstanceStates=['RUNNING'],
        InstanceGroupTypes=['MASTER', 'CORE', 'TASK'],
        PaginationConfig={
            'MaxItems': 100
        }
    )
    for page in response_iterator:
        for instance in page['Instances']:
            instances.append(Instance(instance_id=instance['Ec2InstanceId'],
                                      host_name=instance['PublicDnsName'] or instance['PrivateDnsName']))
    return instances


def get_instances_proxy(url: str, cluster: Cluster, dc='uswest7'):
    url = f'http://{url}/portal/get?dc={dc}&cluster={cluster.id}'
    response = requests.get(url, timeout=5)
    data = response.json()
    instances = []
    for ip in data['core_cluster_ip'] + data['task_cluster_ip']:
        instances.append(Instance(instance_id=f'{cluster.id},{ip}', host_name=ip))
    return instances


def get_instances(cluster: Cluster):
    url = os.getenv('proxy_url')
    try:
        instances = get_instances_proxy(url, cluster)
    except Exception as e:
        logger.info('Could not get instances from proxy. Trying get instances from native API.')
        instances = get_instances_native(cluster)
    return instances


def get_cpu_seconds(instance):
    node_export_url = f'http://{instance.host_name}:9100/metrics'
    total_seconds = 0
    idle_seconds = 0
    for line in requests.get(node_export_url).text.splitlines():
        if line.startswith('node_cpu_seconds_total'):
            seconds = float(line.split(' ')[1])
            total_seconds += seconds
            if 'mode="idle"' in line:
                idle_seconds += seconds
    return total_seconds, idle_seconds


def get_cpu_utilization(instances, period):
    old_cpu_seconds = 0
    old_idle_seconds = 0
    new_cpu_seconds = 0
    new_idle_seconds = 0
    with Session() as session:
        for instance in instances:
            cpu_usage: CpuUsage = session.query(CpuUsage).\
                                     filter(CpuUsage.instance_id == instance.instance_id,
                                            CpuUsage.event_time > (datetime.utcnow() - timedelta(minutes=period))).\
                                     order_by(CpuUsage.event_time).first()
            old_cpu_seconds += cpu_usage.total_seconds if cpu_usage else 0
            old_idle_seconds += cpu_usage.idle_seconds if cpu_usage else 0
            total_seconds, idle_seconds = get_cpu_seconds(instance)
            session.add(CpuUsage(instance_id=instance.instance_id,
                                 event_time=datetime.utcnow(),
                                 total_seconds=total_seconds,
                                 idle_seconds=idle_seconds))
            if cpu_usage:
                new_cpu_seconds += total_seconds
                new_idle_seconds += idle_seconds
        session.commit()
    if new_cpu_seconds == 0:
        raise Exception("No CPU usage is found. This may happen in the first run. Please wait for the next run.")
    return 1 - (new_idle_seconds - old_idle_seconds) / (new_cpu_seconds - old_cpu_seconds)


def get_latest_ready_time(instances):
    latest_ready_time = datetime.min
    for instance in instances:
        ready_time = instance['Status']['Timeline']['ReadyDateTime']
        ready_time = ready_time.astimezone(timezone.utc).replace(tzinfo=None)
        latest_ready_time = max(latest_ready_time, ready_time)
    return latest_ready_time


def update_cluster_status(cluster: Cluster):
    instances = get_instances(cluster)
    cluster.cpu_usage = get_cpu_utilization(instances, cluster.cpu_usage_period_minutes)
    # cluster.instances_latest_ready_time = get_latest_ready_time(instances)


def do_run(cluster: Cluster, dry_run, session):
    response = emr_client.describe_cluster(ClusterId=cluster.id)
    if response['Cluster']['Status']['State'] not in ('RUNNING', 'WAITING'):
        logger.info(f'Skipping cluster {cluster.id} because it is not running.')
        return
    cluster.cluster_name = response['Cluster']['Name']
    master_public_dns = response['Cluster']['MasterPublicDnsName']
    cluster.yarn_metrics = get_current_yarn_metrics(master_public_dns)
    cluster.master_dns_name = master_public_dns
    cluster.current_managed_scaling_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster.id)['ManagedScalingPolicy']
    if cluster.is_fleet:
        cluster.instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
    else:
        cluster.instance_groups = emr_client.list_instance_groups(ClusterId=cluster.id)['InstanceGroups']
    update_cluster_status(cluster)
    # logger.info(f'Cluster {cluster.id} information: \n{cluster.get_info_str()}')
    resize_cluster(cluster, dry_run)


def clean(session):
    session.query(CpuUsage).filter(CpuUsage.event_time < (datetime.utcnow() - timedelta(days=1))).delete(synchronize_session=False)
    session.query(EMREvent).filter(EMREvent.event_time < (datetime.utcnow() - timedelta(days=1))).delete(synchronize_session=False)


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
                logger.info(f'\n####################################### Start {cluster_id} ##########################################')
                cluster = session.get(Cluster, cluster_id)
                do_run(cluster, dry_run, session)
                session.commit()
                logger.info(f'\n####################################### End {cluster_id} ##########################################\n\n\n')
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
