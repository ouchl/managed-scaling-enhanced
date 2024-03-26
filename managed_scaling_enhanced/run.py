from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from managed_scaling_enhanced import metrics
from managed_scaling_enhanced.database import Session
import time
from managed_scaling_enhanced.scale import get_scale_in_flags, get_scale_out_flags, scale_out, scale_in
from managed_scaling_enhanced.models import Cluster, Event
import logging
import orjson

logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')


def update_cluster_status(cluster_id: str, session=Session()):
    try:
        cluster_info = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']
        ms_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster_id)['ManagedScalingPolicy']
    except ClientError as e:
        code = e.response['Error']['Code']
        if code == 'InvalidRequestException':
            logging.warning(f'Cluster {cluster_id} is not valid.')
            return
        else:
            raise e
    cluster: Cluster = session.get(Cluster, cluster_id)
    if not cluster:
        logger.warning(f'Cluster {cluster_id} does not exist in database.')
        return
    cluster.cluster_name = cluster_info['Name']
    cluster.cluster_info = orjson.dumps(cluster_info).decode("utf-8")
    cluster.managed_scaling_policy = orjson.dumps(ms_policy).decode("utf-8")
    session.commit()


def do_run(run_id: int, cluster_id: str, session):
    cluster: Cluster = session.get(Cluster, cluster_id)
    logger.info(f'Getting cluster {cluster.id} metrics...')
    cluster_metrics = metrics.get_metrics(cluster)
    session.add(Event(run_id=run_id, action='GetMetrics', cluster_id=cluster.id,
                      event_time=datetime.utcnow(), data=str(cluster_metrics.__dict__)))
    session.commit()
    scale_out_flags = get_scale_out_flags(cluster, cluster_metrics)
    session.add(Event(run_id=run_id, action='GetScaleOutFlags', cluster_id=cluster.id,
                      event_time=datetime.utcnow(), data=str(scale_out_flags.__dict__)))
    scale_in_flags = get_scale_in_flags(cluster, cluster_metrics)
    session.add(Event(run_id=run_id, action='GetScaleInFlags', cluster_id=cluster.id,
                      event_time=datetime.utcnow(), data=str(scale_in_flags.__dict__)))
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
    session.commit()


def run():
    run_id = int(time.time())
    session = Session()
    clusters = session.query(Cluster).all()
    for cluster in clusters:
        logger.info(f'Updating cluster {cluster.id} status...')
        update_cluster_status(cluster_id=cluster.id, session=session)
    session.commit()
    active_clusters = [cluster.id for cluster in clusters
                       if cluster.cluster_info_obj and cluster.cluster_info_obj['Status']['State'] in ('RUNNING', 'WAITING')]
    session.close()
    for cluster_id in active_clusters:
        try:
            with Session() as session:
                do_run(run_id=run_id, cluster_id=cluster_id, session=session)
        except Exception as e:
            logger.error(f'Cluster {cluster_id} error: {e}')


if __name__ == '__main__':
    run()
