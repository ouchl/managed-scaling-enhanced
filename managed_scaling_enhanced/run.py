import json
from datetime import datetime, timedelta
import boto3
from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
import logging
from pathlib import Path
import pprint
import requests
from managed_scaling_enhanced.scale import scale_in


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


def get_current_yarn_metric(dns_name, metric_name):
    response = requests.get(f"http://{dns_name}:8088/ws/v1/cluster/metrics", timeout=5)
    response.raise_for_status()
    metrics = response.json().get('clusterMetrics', {})
    return metrics.get(metric_name, 0)


def get_instances(cluster: Cluster, fleet_type):
    instances = []
    paginator = emr_client.get_paginator('list_instances')
    response_iterator = paginator.paginate(
        ClusterId=cluster.id,
        InstanceFleetType=fleet_type,
        InstanceStates=['RUNNING'],
        PaginationConfig={
            'MaxItems': 100
        }
    )
    for page in response_iterator:
        for instance in page['Instances']:
            instances.append(instance)
    return instances


def update_cpu_usage(cluster: Cluster, ec2_type_cpu_map):
    master_instances = get_instances(cluster, 'MASTER')
    core_instances = get_instances(cluster, 'CORE')
    task_instances = get_instances(cluster, 'TASK')
    task_instance_ids = set(map(lambda x: x['Ec2InstanceId'], task_instances))
    total_used_resource = 0
    total_cpu_count = 0
    task_used_resource = 0
    task_cpu_count = 0
    all_instances = master_instances + core_instances + task_instances
    logger.info(f'There are {len(all_instances)} instances running in cluster {cluster.id}.')
    logger.info(f'There are {len(task_instances)} task instances running in cluster {cluster.id}.')

    for instance in all_instances:
        instance_id = instance['Ec2InstanceId']
        instance_type = instance['InstanceType']
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
        cpu_count = ec2_type_cpu_map[instance_type]
        total_used_resource += response['Datapoints'][0]['Average']/100 * cpu_count
        total_cpu_count += cpu_count
        if instance_id in task_instance_ids:
            task_used_resource += response['Datapoints'][0]['Average'] / 100 * cpu_count
            task_cpu_count += cpu_count
    cluster.total_used_resource = total_used_resource
    cluster.total_cpu_count = total_cpu_count
    cluster.task_used_resource = task_used_resource
    cluster.task_cpu_count = task_cpu_count


#
# def modify_target_capacity(cluster: Cluster, target_capacity, dry_run):
#     instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
#     total_target_capacity = 0
#     for fleet in instance_fleets:
#         if fleet['InstanceFleetType'] != 'TASK':
#             total_target_capacity += fleet['TargetSpotCapacity']
#             total_target_capacity += fleet['TargetOnDemandCapacity']
#         else:
#             current_spot = fleet['TargetSpotCapacity']
#             logger.info(f'Current target spot capacity: {current_spot}. New target spot capacity: {target_spot}')
#             if current_spot <= target_spot:
#                 logging.info(f'Skipping modifying target spot because current spot capacity is {current_spot} '
#                              f'not higher than target spot capacity {target_spot}.')
#             else:
#                 if not dry_run:
#                     emr_client.modify_instance_fleet(
#                         ClusterId=cluster.id,
#                         InstanceFleet={
#                             'InstanceFleetId': fleet['Id'],
#                             'TargetOnDemandCapacity': 0,
#                             'TargetSpotCapacity': target_spot
#                         }
#                     )
#                     logger.info(f'Modified target spot capacity to {target_spot}.')
#                 else:
#                     logger.info(f'Target spot capacity is not modified because dry run mode is enabled')
#
#
def do_run(cluster, dry_run):
    response = emr_client.describe_cluster(ClusterId=cluster.id)
    if response['Cluster']['Status']['State'] not in ('RUNNING', 'WAITING'):
        logger.info(f'Skipping cluster {cluster.id} because it is not running.')
        return
    cluster.cluster_name = response['Cluster']['Name']
    master_public_dns = response['Cluster']['MasterPublicDnsName']
    cluster.yarn_apps_pending = get_current_yarn_metric(master_public_dns, 'appsPending')
    cluster.yarn_apps_running = get_current_yarn_metric(master_public_dns, 'appsRunning')
    cluster.yarn_total_virtual_cores = get_current_yarn_metric(master_public_dns, 'totalVirtualCores')
    cluster.yarn_reserved_virtual_cores = get_current_yarn_metric(master_public_dns, 'reservedVirtualCores')
    cluster.yarn_total_memory_mb = get_current_yarn_metric(master_public_dns, 'totalMB')
    cluster.yarn_available_memory_mb = get_current_yarn_metric(master_public_dns, 'availableMB')
    cluster.yarn_containers_pending = get_current_yarn_metric(master_public_dns, 'containersPending')
    cluster.managed_scaling_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster.id)['ManagedScalingPolicy']
    cluster.instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
    update_cpu_usage(cluster, get_ec2_types())
    logger.info(f'Cluster {cluster.id} information: \n{pprint.pformat(cluster.to_dict())}')
    scale_in(cluster, dry_run)


def run(dry_run):
    session = Session()
    clusters = session.query(Cluster).all()
    cluster_ids = [cluster.id for cluster in clusters]
    session.close()
    for cluster_id in cluster_ids:
        logger.info(f'Start evaluating cluster {cluster_id}.')
        try:
            with Session() as session:
                cluster = session.get(Cluster, cluster_id)
                do_run(cluster, dry_run)
                if not dry_run:
                    session.commit()
        except Exception as e:
            logger.exception(f'Cluster {cluster_id} error: {e}')


if __name__ == '__main__':
    run(dry_run=True)
