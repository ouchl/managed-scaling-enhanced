import json
from datetime import datetime, timedelta
import boto3
from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
import logging
from pathlib import Path


logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')
ec2_client = boto3.client('ec2')
cw_client = boto3.client('cloudwatch')


def get_ec2_types():
    cache_path = Path('ec2_types.json')
    if cache_path.exists():
        with open(cache_path, 'r') as f:
            ec2_type_cpu_map = json.load(f)
    else:
        ec2_type_cpu_map = {}
        paginator = ec2_client.get_paginator('describe_instance_types')
        page_iterator = paginator.paginate()

        for page in page_iterator:
            for instance_type in page['InstanceTypes']:
                ec2_type_cpu_map[instance_type['InstanceType']] = instance_type['VCpuInfo']['DefaultVCpus']
        with open(cache_path, 'w') as f:
            json.dump(ec2_type_cpu_map, f)
    return ec2_type_cpu_map


def get_task_cpu_usage(cluster: Cluster, ec2_type_cpu_map):
    total_used_resource = 0
    total_cpu_count = 0
    nodes = emr_client.list_instances(
        ClusterId=cluster.id,
        InstanceGroupTypes=['TASK'],
        InstanceStates=['RUNNING']
    )
    node_count = len(nodes['Instances'])
    if node_count == 0:
        logger.info(f'Skipping cluster {cluster.id} because no task instances were found.')
        return total_used_resource, total_cpu_count
    logger.info(f'There are {node_count} task nodes running in cluster {cluster.id}.')
    for node in nodes['Instances']:
        instance_id = node['Ec2InstanceId']
        response = cw_client.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='CPUUtilization',
            Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
            StartTime=datetime.utcnow() - timedelta(minutes=cluster.cpu_usage_period_minutes),
            EndTime=datetime.utcnow(),
            Period=int(cluster.cpu_usage_period_minutes*60+300),
            Statistics=['Average'],
        )
        if len(response['Datapoints']) == 0:
            logger.info(f'Skipping instance {instance_id} because cloud watch metrics were not found.')
            continue
        cpu_count = ec2_type_cpu_map[node['InstanceType']]
        total_used_resource += response['Datapoints'][0]['Average']/100 * cpu_count
        total_cpu_count += cpu_count
    return total_used_resource, total_cpu_count


def modify_target_spot(cluster: Cluster, target_spot):
    instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
    for fleet in instance_fleets:
        if fleet['InstanceFleetType'] == 'TASK':
            current_spot = fleet['TargetSpotCapacity']
            logger.info(f'Current target spot capacity: {current_spot}. New target spot capacity: {target_spot}')
            if current_spot <= target_spot:
                logging.info(f'Skipping modifying target spot because current spot capacity is {current_spot} '
                             f'not higher than target spot capacity {target_spot}.')
            else:
                emr_client.modify_instance_fleet(
                    ClusterId=cluster.id,
                    InstanceFleet={
                        'InstanceFleetId': fleet['Id'],
                        'TargetOnDemandCapacity': 0,
                        'TargetSpotCapacity': target_spot
                    }
                )
                logger.info(f'Modified target spot capacity to {target_spot}.')


def do_run(cluster_id, session):
    cluster = session.get(Cluster, cluster_id)
    current_time = datetime.utcnow()
    last_scale_in_ts = cluster.last_scale_in_ts or datetime.min
    logger.info(f'Last scale in time: {last_scale_in_ts}')
    if (current_time - last_scale_in_ts).total_seconds() < cluster.cool_down_period_minutes*60:
        logger.info(f"Skipping scale in due to cooldown period ({cluster.cool_down_period_minutes} minutes).")
        return
    ec2_type_cpu_map = get_ec2_types()
    total_used_resource, total_cpu_count = get_task_cpu_usage(cluster, ec2_type_cpu_map)
    if total_used_resource == 0:
        return
    avg_cpu_usage = total_used_resource/total_cpu_count
    logger.info(f'Total used resource: {total_used_resource}. '
                f'Total CPU count: {total_cpu_count}. '
                f'Average CPU usage: {avg_cpu_usage}')
    if avg_cpu_usage >= cluster.cpu_usage_lower_bound:
        logger.info(f'CPU usage higher than threshold {cluster.cpu_usage_lower_bound}. Do not scale in.')
    else:
        logger.info(f'CPU usage lower than threshold {cluster.cpu_usage_lower_bound}. Start scaling in.')
        target_spot = int(total_used_resource / cluster.cpu_usage_upper_bound)
        modify_target_spot(cluster, target_spot)
        cluster.last_scale_in_ts = datetime.utcnow()


def run():
    logger.info('Getting ec2 types...')
    get_ec2_types()
    session = Session()
    clusters = session.query(Cluster).all()
    cluster_ids = [cluster.id for cluster in clusters]
    session.close()
    for cluster_id in cluster_ids:
        logger.info(f'Start evaluating cluster {cluster_id}.')
        try:
            with Session() as session:
                do_run(cluster_id, session)
                session.commit()
        except Exception as e:
            logger.exception(f'Cluster {cluster_id} error: {e}')


if __name__ == '__main__':
    run()
