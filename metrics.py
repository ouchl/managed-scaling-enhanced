import boto3
from decimal import Decimal
from datetime import datetime, timedelta
from models import Cluster
import statistics
from dataclasses import dataclass
from typing import Optional
import requests
import random


cw_client = boto3.client('cloudwatch')
emr_client = boto3.client('emr')


@dataclass
class Metric:
    ScaleInAvgPendingAppNum: Optional[int]
    ScaleOutAvgPendingAppNum: Optional[int]
    ScaleInAvgYARNMemoryAvailablePercentage: Optional[Decimal]
    ScaleOutAvgYARNMemoryAvailablePercentage: Optional[Decimal]
    ScaleInAvgCapacityRemainingGB: Optional[Decimal]
    ScaleOutAvgCapacityRemainingGB: Optional[Decimal]
    ScaleInAvgTaskNodeCPULoad: Optional[Decimal]
    ScaleOutAvgTaskNodeCPULoad: Optional[Decimal]
    current_apps_pending: Optional[int]
    current_total_virtual_cores: Optional[int]
    current_apps_running: Optional[int]
    current_reserved_virtual_cores: Optional[int]


def get_current_yarn_metric(cluster: Cluster, metric_name):
    cluster_details = cluster.cluster_info_obj

    # 获取所有主节点的公共DNS
    if 'MasterPublicDnsNameList' in cluster_details:
        # 多主节点架构
        master_public_dns_list = [instance['PublicDnsName'] for instance in cluster_details['MasterPublicDnsNameList']]
    else:
        # 单主节点架构
        master_public_dns_list = [cluster_details['MasterPublicDnsName']]

    # 构造所有YARN ResourceManager URLs
    yarn_rm_urls = [f'http://{master_public_dns}:8088' for master_public_dns in master_public_dns_list]

    # 随机选择一个URL
    random_yarn_rm_url = random.choice(yarn_rm_urls)

    response = requests.get(f"{random_yarn_rm_url}/ws/v1/cluster/metrics", timeout=5)
    response.raise_for_status()
    metrics = response.json().get('clusterMetrics', {})
    return metrics.get(metric_name, 0)


def get_avg_yarn_metric(cluster_id, metric_name, minutes):
    response = cw_client.get_metric_statistics(
        Namespace='AWS/ElasticMapReduce',
        MetricName=metric_name,
        Dimensions=[{'Name': 'JobFlowId', 'Value': cluster_id}],
        StartTime=datetime.utcnow() - timedelta(minutes=minutes),
        EndTime=datetime.utcnow(),
        Period=60,
        Statistics=['Average'],
    )
    metrics = [metric['Average'] for metric in response['Datapoints']]
    if metrics:
        return statistics.mean(metrics)
    else:
        return None


def get_avg_task_cpu_load(cluster_id, minutes):
    nodes = emr_client.list_instances(
        ClusterId=cluster_id,
        InstanceGroupTypes=['TASK'],
        InstanceStates=['RUNNING']
    )
    nodes = [node['Ec2InstanceId'] for node in nodes['Instances']]
    metrics = []
    dimensions = []
    for index, node in enumerate(nodes):
        dimensions.append({'Name': 'InstanceId', 'Value': node})
        if len(dimensions) == 30 or index == len(nodes) - 1:
            response = cw_client.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=dimensions,
                StartTime=datetime.utcnow() - timedelta(minutes=minutes),
                EndTime=datetime.utcnow(),
                Period=60,
                Statistics=['Average'],
            )
            metrics.append([metric['Average'] for metric in response['Datapoints']])
            dimensions.clear()
    if metrics:
        # 对每个位置的指标值进行平均
        avg_metrics = []
        max_length = max(len(batch) for batch in metrics)
        for i in range(max_length):
            values = [batch[i] for batch in metrics if i < len(batch)]
            avg_metrics.append(sum(values) / len(values))

        return statistics.mean(avg_metrics)
    else:
        return None


def get_metrics(cluster: Cluster):
    apps_pending = get_current_yarn_metric(cluster, 'appsPending')
    total_virtual_cores = get_current_yarn_metric(cluster, 'totalVirtualCores')
    apps_running = get_current_yarn_metric(cluster, 'appsRunning')
    reserved_virtual_cores = get_current_yarn_metric(cluster, 'reservedVirtualCores')
    ScaleInAvgCapacityRemainingMB = get_avg_yarn_metric(cluster_id=cluster.id,
                                                        metric_name='MemoryAvailableMB',
                                                        minutes=cluster.config_obj.scaleInAvgCapacityRemainingGBMinutes)
    ScaleOutAvgCapacityRemainingMB = get_avg_yarn_metric(cluster_id=cluster.id,
                                                         metric_name='MemoryAvailableMB',
                                                         minutes=cluster.config_obj.scaleOutAvgCapacityRemainingGBMinutes)
    metrics = Metric(
        ScaleInAvgPendingAppNum=get_avg_yarn_metric(cluster_id=cluster.id,
                                                    metric_name='AppsPending',
                                                    minutes=cluster.config_obj.scaleInAvgPendingAppNumMinutes),
        ScaleOutAvgPendingAppNum=get_avg_yarn_metric(cluster_id=cluster.id,
                                                     metric_name='AppsPending',
                                                     minutes=cluster.config_obj.scaleOutAvgPendingAppNumMinutes),
        ScaleInAvgYARNMemoryAvailablePercentage=get_avg_yarn_metric(cluster_id=cluster.id,
                                                                    metric_name='YARNMemoryAvailablePercentage',
                                                                    minutes=cluster.config_obj.scaleInAvgYARNMemoryAvailablePercentageMinutes),
        ScaleOutAvgYARNMemoryAvailablePercentage=get_avg_yarn_metric(cluster_id=cluster.id,
                                                                     metric_name='YARNMemoryAvailablePercentage',
                                                                     minutes=cluster.config_obj.scaleOutAvgYARNMemoryAvailablePercentageMinutes),
        ScaleInAvgCapacityRemainingGB=ScaleInAvgCapacityRemainingMB / 1024 if ScaleInAvgCapacityRemainingMB else None,
        ScaleOutAvgCapacityRemainingGB=ScaleOutAvgCapacityRemainingMB / 1024 if ScaleOutAvgCapacityRemainingMB else None,
        ScaleInAvgTaskNodeCPULoad=get_avg_task_cpu_load(cluster_id=cluster.id,
                                                        minutes=cluster.config_obj.scaleInAvgTaskNodeCPULoadMinutes),
        ScaleOutAvgTaskNodeCPULoad=get_avg_task_cpu_load(cluster_id=cluster.id,
                                                         minutes=cluster.config_obj.scaleOutAvgTaskNodeCPULoadMinutes),
        current_apps_pending=apps_pending,
        current_apps_running=apps_running,
        current_total_virtual_cores=total_virtual_cores,
        current_reserved_virtual_cores=reserved_virtual_cores
    )
    return metrics


if __name__ == '__main__':
    from database import Session
    print(emr_client.describe_cluster(ClusterId='j-1SJOW088JSHLK'))
    #
    # session = Session()
    # cluster = session.get(Cluster, 'j-1SJOW088JSHLK')
    # cluster.modify_scaling_policy(100)
    # session.commit()
    # print(get_metrics(cluster))
