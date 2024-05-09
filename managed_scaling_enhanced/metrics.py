import asyncio

import aiohttp
import boto3
from managed_scaling_enhanced.models import Cluster, Metric, AvgMetric
from dataclasses import dataclass
import requests
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import inspect
import statistics

logger = logging.getLogger(__name__)

emr_client = boto3.client('emr')


@dataclass
class Instance:
    instance_id: str
    host_name: str


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


def get_instances_proxy(host: str, cluster: Cluster):
    url = f'http://{host}/portal/emrautoscaling?cluster_id={cluster.id}'
    response = requests.get(url, timeout=5)
    data = response.json()
    instances = []
    ips = data['CORE'] + data['MASTER']
    if 'TASK' in data:
        ips += data['TASK']
    for ip in ips:
        instances.append(Instance(instance_id=f'{cluster.id},{ip}', host_name=ip))
    return instances


def get_instances(cluster: Cluster):
    host = os.getenv('api_host')
    try:
        instances = get_instances_proxy(host, cluster)
    except Exception as e:
        logger.warning(f'Could not get instances from host {host}. Error: {e}. Trying get instances from native API.')
        instances = get_instances_native(cluster)
    return instances


async def fetch_cpu_time(session, url):
    async with session.get(url) as response:
        return await response.text()


def get_current_yarn_metrics(dns_name):
    response = requests.get(f"http://{dns_name}:8088/ws/v1/cluster/metrics", timeout=5)
    response.raise_for_status()
    return {k: v for k, v in response.json().get('clusterMetrics', {}).items() if not k.endswith('AcrossPartition')}


def get_cpu_time(cluster: Cluster, metric: Metric):
    instances = get_instances(cluster)
    url_list = [f'http://{instance.host_name}:9100/metrics' for instance in instances]

    # logger.info(url_list)

    async def fetch_all():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_cpu_time(session, url) for url in url_list]
            return await asyncio.gather(*tasks)

    result = asyncio.run(fetch_all())

    total_seconds = 0
    idle_seconds = 0
    for response in result:
        for line in response.splitlines():
            if line.startswith('node_cpu_seconds_total'):
                seconds = float(line.split(' ')[1])
                total_seconds += seconds
                if 'mode="idle"' in line:
                    idle_seconds += seconds
    metric.total_cpu_seconds = total_seconds
    metric.idle_cpu_seconds = idle_seconds


def collect_metrics(cluster: Cluster):
    metric = Metric()
    metric.cluster_id = cluster.id
    get_cpu_time(cluster, metric)
    yarn_metrics = get_current_yarn_metrics(cluster.master_dns_name)
    metric.yarn_app_pending = yarn_metrics.get('appsPending')
    metric.yarn_app_running = yarn_metrics.get('appsRunning')
    metric.yarn_reserved_mem = yarn_metrics.get('reservedMB')
    metric.yarn_available_mem = yarn_metrics.get('availableMB')
    metric.yarn_pending_mem = yarn_metrics.get('pendingMB')
    metric.yarn_allocated_mem = yarn_metrics.get('allocatedMB')
    metric.yarn_total_mem = yarn_metrics.get('totalMB')
    metric.yarn_pending_vcore = yarn_metrics.get('pendingVirtualCores')
    metric.yarn_allocated_vcore = yarn_metrics.get('allocatedVirtualCores')
    metric.yarn_available_vcore = yarn_metrics.get('availableVirtualCores')
    metric.yarn_reserved_vcore = yarn_metrics.get('reservedVirtualCores')
    metric.yarn_total_vcore = yarn_metrics.get('totalVirtualCores')
    metric.event_time = datetime.utcnow()
    return metric


def get_lookback_metrics(cluster, session):
    metrics = (session.query(Metric).filter(Metric.cluster_id == cluster.id,
                                            Metric.event_time > (datetime.utcnow() - timedelta(
                                                minutes=cluster.cpu_usage_period_minutes)))
               .order_by(Metric.event_time)
               .all())
    return metrics


def collect_avg_metrics(cluster, lb_metrics):
    avg_metric = AvgMetric()
    avg_metric.lookback_period = cluster.cpu_usage_period_minutes
    avg_metric.cluster_id = cluster.id
    avg_metric.event_time = datetime.utcnow()
    for field in inspect(AvgMetric).columns.keys():
        if field.startswith('yarn'):
            data = [getattr(metric, field) for metric in lb_metrics]
            setattr(avg_metric, field, statistics.mean(data))
    idle_time = lb_metrics[-1].idle_cpu_seconds - lb_metrics[0].idle_cpu_seconds
    total_time = lb_metrics[-1].total_cpu_seconds - lb_metrics[0].total_cpu_seconds
    avg_metric.cpu_utilization = (total_time-idle_time) / total_time
    return avg_metric
