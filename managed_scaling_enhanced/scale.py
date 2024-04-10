from managed_scaling_enhanced.models import Cluster
import logging
from datetime import datetime
import boto3
import math

logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')


def scale_in(cluster: Cluster, dry_run: bool = False) -> bool:
    flag = False
    if cluster.task_cpu_count == 0:
        logger.info(f'Skipping cluster {cluster.id} scaling in because there are no running task instances.')
    elif cluster.task_instance_fleet_status != 'RUNNING':
        logger.info(
            f'Skipping cluster {cluster.id} scaling in because task instance fleet is {cluster.task_instance_fleet_status}')
    elif (datetime.utcnow() - cluster.task_fleet_latest_ready_time).total_seconds() < cluster.cool_down_period_minutes * 60:
        logger.info(f'Skipping cluster {cluster.id} scaling in because task instance fleet ready time '
                    f'{cluster.task_fleet_latest_ready_time} is within cool down minutes {cluster.cool_down_period_minutes}')
    elif cluster.avg_task_cpu_usage >= cluster.cpu_usage_lower_bound:
        logger.info(
            f'Skipping cluster {cluster.id} scaling in because average CPU usage {cluster.avg_task_cpu_usage} is higher than {cluster.cpu_usage_lower_bound}')
    else:
        logger.info(f'Averaging task CPU usage for cluster {cluster.id} is {cluster.avg_task_cpu_usage}')
        logger.info(f'Task cpu usage lower bound is {cluster.cpu_usage_lower_bound}')
        # the minimum task capacity
        min_task_capacity = max(cluster.current_min_units - cluster.current_max_core_units, 0)
        target_capacity = math.ceil(cluster.task_used_resource / cluster.cpu_usage_upper_bound)
        target_capacity = max(target_capacity, min_task_capacity)
        scale_in_step = cluster.task_target_od_capacity + cluster.task_target_spot_capacity - target_capacity
        logger.info(f'New target capacity is {target_capacity}')
        logger.info(f'Current target OD capacity is {cluster.task_target_od_capacity}.')
        logger.info(f'Current target spot capacity is {cluster.task_target_spot_capacity}.')
        logger.info(f'Capacity to be reduced is {scale_in_step}.')
        if scale_in_step <= 0:
            logger.info(f'Skipping cluster {cluster.id} scaling in due to capacity to be reduced is not positive.')
        else:
            logger.info(f'Starting cluster {cluster.id} scaling in...')
            new_od_capacity = cluster.task_target_od_capacity
            new_spot_capacity = cluster.task_target_spot_capacity
            if new_spot_capacity >= scale_in_step:
                new_spot_capacity -= scale_in_step
            else:
                scale_in_step -= new_spot_capacity
                new_spot_capacity = 0
                new_od_capacity -= scale_in_step
            logger.info(
                f'New target OD capacity is {new_od_capacity}. New target spot capacity is {new_spot_capacity}.')
            new_max_units = target_capacity + cluster.current_max_core_units + 1
            logger.info(f'Modifying managed scaling policy max units from {cluster.current_max_units} to {new_max_units}.')
            cluster.modify_scaling_policy(max_units=new_max_units)
            logger.info(f'Modifying task fleet target OD capacity from {cluster.task_target_od_capacity} to {new_od_capacity}.')
            logger.info(f'Modifying task fleet target spot capacity from {cluster.task_target_spot_capacity} to {new_spot_capacity}.')
            if not dry_run:
                emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                                      ManagedScalingPolicy=cluster.current_managed_scaling_policy)
                emr_client.modify_instance_fleet(ClusterId=cluster.id,
                                                 InstanceFleet={
                                                     'InstanceFleetId': cluster.task_instance_fleet['Id'],
                                                     'TargetOnDemandCapacity': new_od_capacity,
                                                     'TargetSpotCapacity': new_spot_capacity
                                                 })
            flag = True

    return flag


def scale_out(cluster: Cluster, dry_run):
    flag = False
    if cluster.task_instance_fleet_status != 'RUNNING':
        logger.info(
            f'Skipping cluster {cluster.id} scaling out because task instance fleet is {cluster.task_instance_fleet_status}')
    elif (datetime.utcnow() - cluster.task_fleet_latest_ready_time).total_seconds() < cluster.cool_down_period_minutes * 60:
        logger.info(f'Skipping cluster {cluster.id} scaling out because task instance fleet ready time '
                    f'{cluster.task_fleet_latest_ready_time} is within cool down minutes {cluster.cool_down_period_minutes}')
    elif cluster.avg_total_cpu_usage < cluster.cpu_usage_upper_bound:
        logger.info(
            f'Skipping cluster {cluster.id} scaling out because average CPU usage {cluster.avg_total_cpu_usage} is lower than {cluster.cpu_usage_upper_bound}')
    else:
        logger.info(f'Averaging total CPU usage for cluster {cluster.id} is {cluster.avg_total_cpu_usage}')
        logger.info(f'Task cpu usage upper bound is {cluster.cpu_usage_upper_bound}')
        logger.info(f'Starting cluster {cluster.id} scaling out...')
        logger.info(f'Initial managed policy max units: {cluster.initial_max_units}')
        logger.info(f'Current managed policy max units: {cluster.current_max_units}')
        new_max_unit = min(cluster.initial_max_units, cluster.current_max_units * (cluster.avg_total_cpu_usage / cluster.cpu_usage_lower_bound))
        new_max_unit = math.ceil(new_max_unit)
        cluster.modify_scaling_policy(max_units=new_max_unit)
        logger.info(f'New managed policy max units: {new_max_unit}')
        if not dry_run:
            emr_client.put_managed_scaling_policy(ClusterId=cluster.id,
                                                  ManagedScalingPolicy=cluster.current_managed_scaling_policy)
        flag = True
    return flag
