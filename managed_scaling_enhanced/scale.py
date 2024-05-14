from typing import List

from managed_scaling_enhanced import boto3_config
from managed_scaling_enhanced.models import Cluster, AvgMetric, ResizePolicy, Event
import logging
from datetime import datetime
import boto3
import math
from dataclasses import dataclass, asdict
from tabulate import tabulate
from managed_scaling_enhanced.utils import ec2_types
from sqlalchemy import desc

logger = logging.getLogger(__name__)
emr_client = boto3.client('emr', config=boto3_config)


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


def check_requirements(cluster: Cluster, metric: AvgMetric):
    results = []
    # has running task instances to scale in
    # result = ResizeCheckResult(scope='Scale In',
    #                            description='Task instance number',
    #                            flag=cluster.current_task_total_capacity > 0,
    #                            message='The cluster must have at least one task instance to scale in.')
    # results.append(result)

    result = ResizeCheckResult(scope='Both',
                               description='Task fleet/instance group status',
                               flag=cluster.not_resizing,
                               message=f'Current task fleet or instance groups must not be resizing.')
    results.append(result)
    last_action_time = max(cluster.last_scale_in_ts, cluster.last_scale_out_ts)
    last_action_seconds = (datetime.utcnow() - last_action_time).total_seconds()
    result = ResizeCheckResult(scope='Both',
                               description='Cool down',
                               flag=last_action_seconds > cluster.cool_down_period_minutes * 60,
                               message=f'Last action time {last_action_time} must be within cool down minutes {cluster.cool_down_period_minutes}')
    results.append(result)

    # result = ResizeCheckResult(scope='Scale In',
    #                            description='CPU utilization',
    #                            flag=metric.cpu_utilization < cluster.cpu_usage_lower_bound,
    #                            message=f'Cluster total CPU usage {metric.cpu_utilization} should be lower than cluster lower bound {cluster.cpu_usage_lower_bound}.')
    # results.append(result)
    #
    # result = ResizeCheckResult(scope='Scale Out',
    #                            description='CPU utilization',
    #                            flag=metric.cpu_utilization > cluster.cpu_usage_upper_bound,
    #                            message=f'Cluster total CPU usage {metric.cpu_utilization} should be higher than cluster upper bound {cluster.cpu_usage_upper_bound}.')
    # results.append(result)
    return results


def resize_cluster(cluster, session, dry_run):
    avg_metric = session.query(AvgMetric).filter(AvgMetric.cluster_id == cluster.id).order_by(desc(AvgMetric.event_time)).first()
    # results = check_requirements(cluster, avg_metric)
    # results_dicts = [asdict(result) for result in results]
    # yarn_metrics_dicts = [{'metric': k, 'value': v} for k, v in cluster.yarn_metrics.items()]
    # table = tabulate(yarn_metrics_dicts, headers="keys", tablefmt="grid")
    logger.info(f'------------------------ Average Yarn Metrics ---------------------\n{avg_metric.__dict__}')
    cluster_status_dicts = [{'key': k, 'value': v} for k, v in cluster.to_dict().items()]
    table = tabulate(cluster_status_dicts, headers="keys", tablefmt="grid")
    logger.info(f'------------------------ Cluster Status ---------------------\n{cluster.to_dict()}')
    # table = tabulate(results_dicts, headers="keys", tablefmt="grid")
    # logger.info(f'------------------------------- Check Results ---------------------------\n{table}')
    target_units = compute_target_max_units(cluster, avg_metric)
    logger.info(f'Computed target units: {target_units}')
    # if target_units == cluster.current_max_units:
    #     logger.info(f'Skip cluster {cluster.id}. Target unit: {target_units}. Current max units: {cluster.current_max_units}')
    #     return

    event = Event()
    event.cluster_id = cluster.id
    event.event_time = datetime.utcnow()
    event.current_max_units = cluster.current_max_units
    event.target_max_units = target_units
    event.is_resizing = cluster.is_resizing
    last_action_time = max(cluster.last_scale_in_ts, cluster.last_scale_out_ts)
    last_action_seconds = (datetime.utcnow() - last_action_time).total_seconds()
    event.is_cooling_down = last_action_seconds < cluster.cool_down_period_minutes * 60
    action_flag = True
    if cluster.is_resizing:
        logger.info(f'Skip resizing cluster {cluster.id}.')
        action_flag = False
    if event.is_cooling_down:
        logger.info(f'Skip cooling down cluster {cluster.id}.')
        action_flag = False
    if dry_run:
        action_flag = True
    action = 'nothing'
    if action_flag:
        if target_units < cluster.current_max_units:
            logger.info(f'------------------- Start to scale in ----------------------')
            scale_in(cluster, target_units, dry_run)
            action = 'scale in'
            logger.info(f'------------------- Finished scaling in ----------------------')
        elif target_units > cluster.current_max_units:
            logger.info(f'------------------- Start to scale out ----------------------')
            scale_out(cluster, target_units, dry_run)
            logger.info(f'------------------- Finished scaling out ----------------------')
            action = 'scale out'

    event.action = action
    session.add(event)
    session.commit()


def log_table_str(data: List[dataclass]):
    dicts = [asdict(item) for item in data]
    return tabulate(dicts, headers="keys", tablefmt="grid")


def log_parameters(parameters):
    logger.info(f'Parameters:\n{log_table_str(parameters)}')


def compute_target_max_units(cluster: Cluster, avg_metric: AvgMetric):
    step = 0
    if cluster.resize_policy == ResizePolicy.CPU_BASED:
        logger.info('Use CPU based policy...')
        if avg_metric.cpu_utilization < cluster.cpu_usage_lower_bound:
            step = - (1 - avg_metric.cpu_utilization / cluster.cpu_usage_upper_bound) * cluster.current_max_units
        elif avg_metric.cpu_utilization > cluster.cpu_usage_upper_bound:
            step = (avg_metric.cpu_utilization / cluster.cpu_usage_upper_bound - 1) * cluster.current_max_units
    elif cluster.resize_policy == ResizePolicy.RESOURCE_BASED:
        logger.info('Use resource based policy...')
        if avg_metric.yarn_pending_vcore > 0 or avg_metric.yarn_pending_mem > 0:
            step1 = (avg_metric.yarn_pending_vcore / avg_metric.yarn_total_vcore) * cluster.current_max_units
            step2 = (avg_metric.yarn_pending_mem / avg_metric.yarn_total_mem) * cluster.current_max_units
            step = max(step1, step2)
        else:
            step1 = - (1 - (avg_metric.yarn_allocated_mem + avg_metric.yarn_reserved_mem) / avg_metric.yarn_total_mem) * cluster.current_max_units
            step2 = - (1 - (avg_metric.yarn_allocated_vcore + avg_metric.yarn_reserved_vcore) / avg_metric.yarn_total_vcore) * cluster.current_max_units
            step = max(step1, step2)
            step = min(step, 0)
    if step > 0:
        step = math.ceil(step * cluster.scale_out_factor)
    elif step < 0:
        step = math.floor(step * cluster.scale_in_factor)
    logger.info(f'Computed step: {step}')
    target_units = cluster.current_max_units + step
    target_units = min(target_units, cluster.max_capacity_limit)
    target_units = max(target_units, cluster.current_min_units + 1)
    target_units = max(target_units, cluster.current_max_core_units)
    target_units = max(target_units, cluster.current_max_od_units)
    return target_units


def scale_in(cluster: Cluster, target_units, dry_run: bool = False) -> bool:
    delta = cluster.current_max_units - target_units
    logger.info(f'Starting cluster {cluster.id} scaling in...')
    changes = [ParameterChange(parameter='MaximumCapacityUnits',
                               before=cluster.current_max_units, after=str(target_units))]

    cluster.modify_scaling_policy(max_units=target_units)

    if not dry_run:
        emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                              ManagedScalingPolicy=cluster.current_managed_scaling_policy)

    if cluster.managed_scaling_unit_type == 'InstanceFleetUnits':
        new_od_capacity = cluster.current_task_od_capacity
        new_spot_capacity = cluster.current_task_spot_capacity
        if new_spot_capacity >= delta:
            new_spot_capacity -= delta
        else:
            delta -= new_spot_capacity
            new_spot_capacity = 0
            new_od_capacity -= delta
        new_od_capacity = max(new_od_capacity, 0)
        changes.append(ParameterChange(parameter='TargetOnDemandCapacity', before=cluster.task_target_od_capacity,
                       after=str(new_od_capacity)))
        changes.append(ParameterChange(parameter='TargetSpotCapacity', before=cluster.task_target_spot_capacity,
                       after=str(new_spot_capacity)))
        if not dry_run:
            emr_client.modify_instance_fleet(ClusterId=cluster.id,
                                             InstanceFleet={
                                                 'InstanceFleetId': cluster.task_instance_fleet['Id'],
                                                 'TargetOnDemandCapacity': new_od_capacity,
                                                 'TargetSpotCapacity': new_spot_capacity
                                             })
    else:
        instance_groups = []
        sorted_groups = sorted(cluster.task_instance_groups, key=lambda x: 0 if x['Market'] == 'SPOT' else 1)
        for group in sorted_groups:
            if cluster.managed_scaling_unit_type == 'Instances':
                units = group['RunningInstanceCount']
            else:
                units = group['RunningInstanceCount'] * ec2_types[group['InstanceType']]
            if units >= delta:
                instance_groups.append({'InstanceGroupId': group['Id'], 'InstanceCount': units - delta})
                break
            else:
                instance_groups.append({'InstanceGroupId': group['Id'],
                                        'InstanceCount': 0})
                delta -= units
        logger.info(f'Instance groups modification: {instance_groups}')
        if not dry_run:
            emr_client.modify_instance_groups(ClusterId=cluster.id, InstanceGroups=instance_groups)

    log_parameters(changes)

    cluster.last_scale_in_ts = datetime.utcnow()
    return True


def scale_out(cluster: Cluster, target_units, dry_run):
    changes = [
        ParameterChange(parameter='MaximumCapacityUnits', before=cluster.current_max_units,
                        after=str(target_units))]
    log_parameters(changes)
    cluster.modify_scaling_policy(max_units=target_units)
    # logger.info(f'New managed policy max units: {new_max_units}')
    if not dry_run:
        emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                              ManagedScalingPolicy=cluster.current_managed_scaling_policy)
    flag = True
    cluster.last_scale_out_ts = datetime.utcnow()
    return flag
