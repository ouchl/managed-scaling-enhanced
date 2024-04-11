import json
from datetime import datetime, timedelta, timezone
import boto3
from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
import logging
from pathlib import Path
import requests
from managed_scaling_enhanced.scale import resize_cluster


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


def get_current_yarn_metrics(dns_name):
    response = requests.get(f"http://{dns_name}:8088/ws/v1/cluster/metrics", timeout=5)
    response.raise_for_status()
    return {k: v for k, v in response.json().get('clusterMetrics', {}).items() if not k.endswith('AcrossPartition')}


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

    task_fleet_ready_time = datetime.min
    for instance in task_instances:
        instance_ready_time = instance['Status']['Timeline']['ReadyDateTime'].astimezone(timezone.utc).replace(tzinfo=None)
        task_fleet_ready_time = max(task_fleet_ready_time, instance_ready_time)
    cluster.task_fleet_latest_ready_time = task_fleet_ready_time

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


def do_run(cluster, dry_run):
    response = emr_client.describe_cluster(ClusterId=cluster.id)
    if response['Cluster']['Status']['State'] not in ('RUNNING', 'WAITING'):
        logger.info(f'Skipping cluster {cluster.id} because it is not running.')
        return
    cluster.cluster_name = response['Cluster']['Name']
    master_public_dns = response['Cluster']['MasterPublicDnsName']
    cluster.yarn_metrics = get_current_yarn_metrics(master_public_dns)
    cluster.current_managed_scaling_policy = emr_client.get_managed_scaling_policy(ClusterId=cluster.id)['ManagedScalingPolicy']
    cluster.instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
    update_cpu_usage(cluster, get_ec2_types())
    logger.info(f'Cluster {cluster.id} information: \n{cluster.get_info_str()}')
    resize_cluster(cluster, dry_run)
    # if scale_in(cluster, dry_run):
    #     cluster.last_scale_in_ts = datetime.utcnow()
    #     logger.info(f'Cluster {cluster.id} scaled in successfully.')
    # elif scale_out(cluster, dry_run):
    #     cluster.last_scale_out_ts = datetime.utcnow()
    #     logger.info(f'Cluster {cluster.id} scaled out successfully.')


def run(dry_run):
    session = Session()
    clusters = session.query(Cluster).all()
    cluster_ids = [cluster.id for cluster in clusters]
    session.close()
    for cluster_id in cluster_ids:
        try:
            with Session() as session:
                logger.info(f'\n####################################### Start {cluster_id} ##########################################')
                cluster = session.get(Cluster, cluster_id)
                do_run(cluster, dry_run)
                if not dry_run:
                    session.commit()
                logger.info(f'\n####################################### End {cluster_id} ##########################################\n\n\n')
        except Exception as e:
            logger.exception(f'Cluster {cluster_id} error: {e}')


if __name__ == '__main__':
    run(dry_run=True)
