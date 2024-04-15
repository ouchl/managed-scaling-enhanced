from typing import List

from managed_scaling_enhanced.models import Cluster
import logging
from datetime import datetime
import boto3
import math
from dataclasses import dataclass, asdict
from tabulate import tabulate

logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')


@dataclass
class ResizeCheckResult:
    scope: str
    description: str
    message: str
    flag: bool


@dataclass
class ParameterChange:
    parameter: str
    before: str
    after: str


def check_requirements(cluster):
    results = []
    # has running task instances to scale in
    result = ResizeCheckResult(scope='Scale In',
                               description='Task instance number',
                               flag=cluster.task_target_spot_capacity > 0,
                               message='The cluster must have at least one task instance to scale in.')
    results.append(result)

    result = ResizeCheckResult(scope='Both',
                               description='Task fleet status',
                               flag=cluster.task_instance_fleet_status == 'RUNNING',
                               message=f'Current task fleet status is {cluster.task_instance_fleet_status}. '
                                       f'The task fleet must be ready to resize the cluster.')
    results.append(result)
    fleet_ready_duration = (datetime.utcnow() - cluster.fleet_latest_ready_time).total_seconds()
    result = ResizeCheckResult(scope='Both',
                               description='Cool down',
                               flag=fleet_ready_duration > cluster.cool_down_period_minutes * 60,
                               message=f'Task instance fleet ready time {cluster.fleet_latest_ready_time} must be within cool down minutes {cluster.cool_down_period_minutes}')
    results.append(result)

    result = ResizeCheckResult(scope='Scale In',
                               description='CPU utilization',
                               flag=cluster.cpu_usage < cluster.cpu_usage_lower_bound,
                               message=f'Cluster total CPU usage {cluster.cpu_usage} should be lower than cluster lower bound {cluster.cpu_usage_lower_bound}.')
    results.append(result)

    result = ResizeCheckResult(scope='Scale Out',
                               description='CPU utilization',
                               flag=cluster.cpu_usage > cluster.cpu_usage_upper_bound,
                               message=f'Cluster total CPU usage {cluster.cpu_usage} should be higher than cluster upper bound {cluster.cpu_usage_upper_bound}.')
    results.append(result)
    return results


def resize_cluster(cluster, dry_run):
    results = check_requirements(cluster)
    results_dicts = [asdict(result) for result in results]
    yarn_metrics_dicts = [{'metric': k, 'value': v} for k, v in cluster.yarn_metrics.items()]
    table = tabulate(yarn_metrics_dicts, headers="keys", tablefmt="grid")
    logger.info(f'------------------------ Yarn Metrics ---------------------\n{table}')
    cluster_status_dicts = [{'key': k, 'value': v} for k, v in cluster.to_dict().items()]
    table = tabulate(cluster_status_dicts, headers="keys", tablefmt="grid")
    logger.info(f'------------------------ Cluster Status ---------------------\n{table}')
    table = tabulate(results_dicts, headers="keys", tablefmt="grid")
    logger.info(f'------------------------------- Check Results ---------------------------\n{table}')
    scale_in_flag = True
    for result in results:
        if result.scope in ('Scale In', 'Both') and not result.flag:
            logger.info(f'Skip scaling in. Reason: {result.message}')
            scale_in_flag = False
            break
    if scale_in_flag:
        logger.info(f'------------------- Start to scale in ----------------------')
        scale_in(cluster, dry_run)
        logger.info(f'------------------- Finished scaling in ----------------------')
        return
    scale_out_flag = True
    for result in results:
        if result.scope in ('Scale Out', 'Both') and not result.flag:
            logger.info(f'Skip scaling out. Reason: {result.message}')
            scale_out_flag = False
            break
    if scale_out_flag:
        logger.info(f'------------------- Start to scale out ----------------------')
        scale_out(cluster, dry_run)
        logger.info(f'------------------- Finished scaling out ----------------------')
        return


def log_table_str(data: List[dataclass]):
    dicts = [asdict(item) for item in data]
    return tabulate(dicts, headers="keys", tablefmt="grid")


def log_parameters(parameters):
    logger.info(f'Parameters:\n{log_table_str(parameters)}')


def scale_in(cluster: Cluster, dry_run: bool = False) -> bool:
    # the minimum task capacity
    min_task_capacity = max(cluster.current_min_units - cluster.current_max_core_units, 0)
    target_capacity = math.floor((cluster.cpu_usage / cluster.cpu_usage_upper_bound) * cluster.task_target_spot_capacity)
    target_capacity = max(target_capacity, min_task_capacity)
    scale_in_step = cluster.task_target_od_capacity + cluster.task_target_spot_capacity - target_capacity
    logger.info(f'Target capacity: {target_capacity}. Minimum task capacity: {min_task_capacity}.')
    if scale_in_step <= 0:
        logger.info(f'Skipping cluster {cluster.id} scaling in. Current capacity {cluster.task_target_spot_capacity} is good.')
        return False
    logger.info(f'Starting cluster {cluster.id} scaling in...')
    new_od_capacity = cluster.task_target_od_capacity
    new_spot_capacity = cluster.task_target_spot_capacity
    if new_spot_capacity >= scale_in_step:
        new_spot_capacity -= scale_in_step
    else:
        scale_in_step -= new_spot_capacity
        new_spot_capacity = 0
        new_od_capacity -= scale_in_step
    new_max_units = target_capacity + cluster.current_max_core_units + 1
    changes = [
        ParameterChange(parameter='MaximumCapacityUnits', before=cluster.current_max_units, after=str(new_max_units)),
        ParameterChange(parameter='TargetOnDemandCapacity', before=cluster.task_target_od_capacity,
                        after=str(new_od_capacity)),
        ParameterChange(parameter='TargetSpotCapacity', before=cluster.task_target_spot_capacity,
                        after=str(new_spot_capacity))]
    log_parameters(changes)
    cluster.modify_scaling_policy(max_units=new_max_units)

    if not dry_run:
        emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                              ManagedScalingPolicy=cluster.current_managed_scaling_policy)
        emr_client.modify_instance_fleet(ClusterId=cluster.id,
                                         InstanceFleet={
                                             'InstanceFleetId': cluster.task_instance_fleet['Id'],
                                             'TargetOnDemandCapacity': new_od_capacity,
                                             'TargetSpotCapacity': new_spot_capacity
                                         })
    return True


def scale_out(cluster: Cluster, dry_run):
    new_max_units = min(cluster.initial_max_units, cluster.current_max_units * (cluster.cpu_usage / cluster.cpu_usage_upper_bound))
    new_max_units = math.ceil(new_max_units)
    changes = [
        ParameterChange(parameter='MaximumCapacityUnits', before=cluster.current_max_units,
                        after=str(new_max_units)),
        ParameterChange(parameter='TargetOnDemandCapacity', before=cluster.task_target_od_capacity,
                        after=cluster.task_target_od_capacity),
        ParameterChange(parameter='TargetSpotCapacity', before=cluster.task_target_spot_capacity,
                        after=cluster.task_target_spot_capacity)]
    log_parameters(changes)
    cluster.modify_scaling_policy(max_units=new_max_units)
    # logger.info(f'New managed policy max units: {new_max_units}')
    if not dry_run:
        emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                              ManagedScalingPolicy=cluster.current_managed_scaling_policy)
    flag = True
    return flag
