import asyncio

import aiohttp
import boto3

from managed_scaling_enhanced import boto3_config
from managed_scaling_enhanced.models import Cluster, Metric, AvgMetric, CpuUsage
from dataclasses import dataclass
import requests
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import inspect
import statistics

logger = logging.getLogger(__name__)

emr_client = boto3.client('emr', config=boto3_config)


@dataclass
class Instance:
    cluster_id: str
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
            instances.append(Instance(cluster_id=cluster.id,
                                      instance_id=instance['Ec2InstanceId'],
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
        instances.append(Instance(cluster_id=cluster.id, instance_id=f'{ip}', host_name=ip))
    return instances


def get_instances(cluster: Cluster):
    host = os.getenv('api_host')
    try:
        instances = get_instances_proxy(host, cluster)
    except Exception as e:
        logger.warning(f'Could not get instances from host {host}. Error: {e}. Trying get instances from native API.')
        instances = get_instances_native(cluster)
    return instances


async def fetch_cpu_time(session, instance):
    url = f'http://{instance.host_name}:9100/metrics'
    try:
        async with session.get(url) as response:
            total_seconds = 0
            idle_seconds = 0
            resp_text = await response.text()
            for line in resp_text.splitlines():
                if line.startswith('node_cpu_seconds_total'):
                    seconds = float(line.split(' ')[1])
                    total_seconds += seconds
                    if 'mode="idle"' in line:
                        idle_seconds += seconds
            cpu_usage = CpuUsage()
            cpu_usage.cluster_id = instance.cluster_id
            cpu_usage.instance_id = instance.instance_id
            cpu_usage.total_seconds = total_seconds
            cpu_usage.idle_seconds = idle_seconds
            cpu_usage.event_time = datetime.utcnow()
            return cpu_usage
    except Exception as e:
        logger.info(f'Error get cpu usage of instance {instance.host_name}. Error: {e}')
        return None


def get_current_yarn_metrics(dns_name):
    response = requests.get(f"http://{dns_name}:8088/ws/v1/cluster/metrics", timeout=5)
    response.raise_for_status()
    return {k: v for k, v in response.json().get('clusterMetrics', {}).items() if not k.endswith('AcrossPartition')}


def get_cpu_utilization(cluster: Cluster, db_session):
    instances = get_instances(cluster)

    async def fetch_all():
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [fetch_cpu_time(session, instance) for instance in instances]
            results = await asyncio.gather(*tasks)
            data_list = []
            for result in results:
                if result is not None:
                    data_list.append(result)
            return data_list

    old_total = 0
    old_busy = 0
    new_total = 0
    new_busy = 0
    cpu_usages = asyncio.run(fetch_all())
    for cpu_usage in cpu_usages:
        old_cpu_usage = (db_session.query(CpuUsage).filter(CpuUsage.cluster_id == cluster.id,
                                                           CpuUsage.instance_id == cpu_usage.instance_id,
                                                           CpuUsage.event_time > (datetime.utcnow() - timedelta(
                                                               minutes=cluster.metrics_lookback_period_minutes)))
                         .order_by(CpuUsage.event_time)
                         .first())
        if old_cpu_usage:
            old_total += old_cpu_usage.total_seconds
            old_busy += old_cpu_usage.busy_seconds
            new_total += cpu_usage.total_seconds
            new_busy += cpu_usage.busy_seconds
    db_session.add_all(cpu_usages)
    db_session.commit()
    if new_total != 0:
        return (new_busy - old_busy) / (new_total - old_total)


def collect_metrics(cluster: Cluster):
    metric = Metric()
    metric.cluster_id = cluster.id
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
    metric.yarn_active_nodes = yarn_metrics.get('activeNodes')
    metric.event_time = datetime.utcnow()
    return metric


def get_lookback_metrics(cluster, session):
    metrics = (session.query(Metric).filter(Metric.cluster_id == cluster.id,
                                            Metric.event_time > (datetime.utcnow() - timedelta(
                                                minutes=cluster.metrics_lookback_period_minutes)))
               .order_by(Metric.event_time)
               .all())
    return metrics


def collect_avg_metrics(cluster, lb_metrics):
    avg_metric = AvgMetric()
    avg_metric.lookback_period = cluster.metrics_lookback_period_minutes
    avg_metric.cluster_id = cluster.id
    avg_metric.event_time = datetime.utcnow()
    for field in inspect(AvgMetric).columns.keys():
        if field.startswith('yarn'):
            data = [getattr(metric, field) for metric in lb_metrics]
            setattr(avg_metric, field, statistics.mean(data))
    return avg_metric
