from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from managed_scaling_enhanced import metrics
from managed_scaling_enhanced.database import Session
import time
from managed_scaling_enhanced.scale import get_scale_in_flags, get_scale_out_flags, scale_out, scale_in
from managed_scaling_enhanced.models import Cluster, Event
import logging
from dataclasses import fields


logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')


def update_cluster_status(cluster_id: str, session=Session()):
    try:
        cluster_info = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']
        ms_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster_id)['ManagedScalingPolicy']
    except ClientError as e:
        code = e.response['Error']['Code']
        if code == 'InvalidRequestException':
            logging.exception(f'Cluster {cluster_id} is not valid.')
            return
        else:
            raise e
    cluster: Cluster = session.get(Cluster, cluster_id)
    if not cluster:
        logger.warning(f'Cluster {cluster_id} does not exist in database.')
        return
    cluster.cluster_name = cluster_info['Name']
    cluster.cluster_info = cluster_info
    cluster.managed_scaling_policy = ms_policy
    session.commit()


def do_run(run_id: int, cluster_id: str, session):
    cluster: Cluster = session.get(Cluster, cluster_id)
    logger.info(f'Getting cluster {cluster.id} metrics...')
    cluster_metrics = metrics.get_metrics(cluster)
    session.add(Event(run_id=run_id, action='GetMetrics', cluster_id=cluster.id,
                      event_time=datetime.utcnow(), data=cluster_metrics
                      ))
    session.commit()
    # 检查metric是否为空
    for field in fields(metrics.Metric):
        if getattr(cluster_metrics, field.name) is None:
            logger.warning(f'Metric {field.name} of cluster {cluster_id} is null. Skip it in this run {run_id}.')
            return
    scale_out_flags = get_scale_out_flags(cluster, cluster_metrics)
    session.add(Event(run_id=run_id, action='GetScaleOutFlags', cluster_id=cluster.id,
                      event_time=datetime.utcnow(), data=scale_out_flags
                      ))
    scale_in_flags = get_scale_in_flags(cluster, cluster_metrics)
    session.add(Event(run_id=run_id, action='GetScaleInFlags', cluster_id=cluster.id,
                      event_time=datetime.utcnow(), data=scale_in_flags
                      ))
    session.commit()

    if scale_out_flags.OverallFlag:
        scaled_out = scale_out(cluster=cluster, metrics=cluster_metrics)
        if scaled_out:
            logger.info(f'Cluster {cluster.id} scaled out successfully.')
            session.add(Event(run_id=run_id, action='ScaleOut', cluster_id=cluster.id,
                              event_time=datetime.utcnow(), data=cluster.managed_scaling_policy))
    elif scale_in_flags.OverallFlag:
        scaled_in = scale_in(cluster=cluster, metrics=cluster_metrics)
        if scaled_in:
            logger.info(f'Cluster {cluster.id} scaled in successfully.')
            session.add(Event(run_id=run_id, action='ScaleIn', cluster_id=cluster.id,
                              event_time=datetime.utcnow(), data=cluster.managed_scaling_policy))
    else:
        logger.info(f'Cluster {cluster.id} is good. Do nothing in this run.')
    session.commit()


def run():
    run_id = int(time.time())
    session = Session()
    clusters = session.query(Cluster).all()
    logger.info(f'Updating cluster status...')
    for cluster in clusters:
        update_cluster_status(cluster_id=cluster.id, session=session)
        session.add(Event(run_id=run_id, action='GetCluster', cluster_id=cluster.id,
                          event_time=datetime.utcnow(), data=cluster.to_dict()))
    session.commit()
    active_clusters = [cluster.id for cluster in clusters
                       if cluster.cluster_info and cluster.cluster_info['Status']['State'] in ('RUNNING', 'WAITING')]
    session.close()
    for cluster_id in active_clusters:
        logger.info(f'Start processing cluster {cluster_id} in run {run_id}...')
        try:
            with Session() as session:
                do_run(run_id=run_id, cluster_id=cluster_id, session=session)
        except Exception as e:
            logger.exception(f'Cluster {cluster_id} error: {e}')


if __name__ == '__main__':
    run()
