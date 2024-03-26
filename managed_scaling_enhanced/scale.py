from managed_scaling_enhanced.models import Cluster
from managed_scaling_enhanced.metrics import Metric
import logging
from dataclasses import dataclass
from datetime import datetime
import boto3


logger = logging.getLogger(__name__)
emr_client = boto3.client('emr')


@dataclass
class ScaleFlags:
    YarnMemPercentageFlag: bool
    FreeMemFlag: bool
    AppsPendingFlag: bool
    CpuLoadFlag: bool
    MaxUnitFlag: bool
    OverallFlag: bool


def get_scale_out_flags(cluster: Cluster, metrics: Metric):
    config = cluster.config_obj
    yarn_mem_percentage_flag = metrics.ScaleOutAvgYARNMemoryAvailablePercentage * 100 <= config.scaleOutAvgYARNMemoryAvailablePercentageValue
    free_mem_flag = metrics.ScaleOutAvgCapacityRemainingGB <= config.scaleOutAvgCapacityRemainingGBValue
    metrics.current_apps_pending_flag = metrics.ScaleOutAvgPendingAppNum >= config.scaleOutAvgPendingAppNumValue
    cpu_load_flag = metrics.ScaleOutAvgTaskNodeCPULoad >= config.scaleOutAvgTaskNodeCPULoadValue
    max_unit_flag = cluster.scaling_policy_max_units < config.maximumUnits
    overall_flag = ((yarn_mem_percentage_flag or free_mem_flag or metrics.current_apps_pending_flag)
                    and cpu_load_flag and max_unit_flag)
    scale_out_flags = ScaleFlags(
        YarnMemPercentageFlag=yarn_mem_percentage_flag,
        FreeMemFlag=free_mem_flag,
        AppsPendingFlag=free_mem_flag,
        CpuLoadFlag=cpu_load_flag,
        MaxUnitFlag=max_unit_flag,
        OverallFlag=overall_flag
    )
    return scale_out_flags


def get_scale_in_flags(cluster: Cluster, metrics: Metric):
    config = cluster.config_obj
    yarn_mem_percentage_flag = metrics.ScaleInAvgYARNMemoryAvailablePercentage * 100 > config.scaleInAvgYARNMemoryAvailablePercentageValue
    free_mem_flag = metrics.ScaleInAvgCapacityRemainingGB > config.scaleInAvgCapacityRemainingGBValue
    metrics.current_apps_pending_flag = metrics.ScaleInAvgPendingAppNum < config.scaleInAvgPendingAppNumValue
    cpu_load_flag = metrics.ScaleInAvgTaskNodeCPULoad < config.scaleInAvgTaskNodeCPULoadValue
    max_unit_flag = cluster.scaling_policy_max_units > config.minimumUnits
    overall_flag = ((yarn_mem_percentage_flag or free_mem_flag or metrics.current_apps_pending_flag or cpu_load_flag)
                    and max_unit_flag)
    scale_in_flags = ScaleFlags(
        YarnMemPercentageFlag=yarn_mem_percentage_flag,
        FreeMemFlag=free_mem_flag,
        AppsPendingFlag=free_mem_flag,
        CpuLoadFlag=cpu_load_flag,
        MaxUnitFlag=max_unit_flag,
        OverallFlag=overall_flag
    )
    return scale_in_flags


def scale_out(cluster: Cluster, metrics: Metric) -> bool:
    current_time = datetime.utcnow()
    config = cluster.config_obj

    # 检查是否在冷却时间内
    if current_time - cluster.last_scale_out_ts < config.scaleOutCooldownSeconds:
        logger.info(
            f"⌛️ Skipping scale out operation due to cooldown period ({config.scaleOutCooldownSeconds} seconds).")
        return False

    # 如果没有等待分配资源的应用程序且集群资源利用率较低,则直接返回
    if metrics.current_apps_pending == 0:
        if metrics.current_reserved_virtual_cores <= 2:
            logger.info(
                "No pending applications and cluster resource utilization is low, skipping scale out operation.")
            return False
        else:
            logger.info(
                "No pending applications, but cluster resource utilization is high, proceeding with scale out operation.")
    else:
        logger.info(f"There are {metrics.current_apps_pending} pending applications, proceeding with scale out operation.")

    # 计算新的 MaximumCapacityUnits
    if metrics.current_apps_pending == 0:
        step = int(metrics.current_reserved_virtual_cores * config.scaleOutFactor)
    else:
        step = int(
            (metrics.current_total_virtual_cores / metrics.current_apps_running) * config.scaleOutFactor
        )
    new_max_capacity_units = cluster.scaling_policy_max_units + step
    new_max_od_capacity_units = cluster.scaling_policy_max_od_units + step

    # 确保新的 MaximumCapacityUnits 大于 MinimumCapacityUnits
    new_max_capacity_units = max(new_max_capacity_units, cluster.scaling_policy_min_units + 1)

    # 确保新的 MaximumCapacityUnits 不超过最大限制
    new_max_capacity_units = min(new_max_capacity_units, config.maximumUnits)
    # 确保新的 MaximumOnDemandCapacityUnits 不超过最大限制
    new_max_od_capacity_units = min(new_max_od_capacity_units, config.maximumOnDemandUnits)

    # 更新 MaximumCapacityUnits
    cluster.modify_scaling_policy(max_units=new_max_capacity_units, max_od_units=new_max_od_capacity_units)
    # 应用新策略
    emr_client.put_managed_scaling_policy(cluster.id, cluster.managed_scaling_policy)
    cluster.last_scale_out_ts = current_time
    return True


def scale_in(cluster: Cluster, metrics: Metric) -> bool:
    current_time = datetime.utcnow()
    config = cluster.config_obj
    if current_time - cluster.last_scale_in_ts < config.scaleInCooldownSeconds:
        logger.info(
            f"⌛️ Skipping scale in operation due to cooldown period ({config.scaleInCooldownSeconds} seconds).")
        return False
    # od 不小于 core nodes 数量
    max_od_units = max(cluster.scaling_policy_max_core_units, cluster.scaling_policy_max_od_units)
    if metrics.current_apps_pending == 0:
        logger.info("No pending applications, setting minimum capacity.")
        # od的数量没有减少，why？
        cluster.modify_scaling_policy(max_units=config.minimumUnits, max_od_units=max_od_units)

    # 如果 apps_pending 不为 0
    else:
        # 计算新的 MaximumCapacityUnits
        new_max_capacity_units = max(config.minimumUnits, cluster.scaling_policy_max_units - int(
            (metrics.current_total_virtual_cores / metrics.current_apps_running) * config.scaleInFactor))
        cluster.modify_scaling_policy(max_units=new_max_capacity_units, max_od_units=max_od_units)

    # 应用新策略
    emr_client.put_managed_scaling_policy(cluster.id, cluster.managed_scaling_policy)
    cluster.last_scale_in_ts = current_time

    # 修改 Instance Fleets
    instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']
    for fleet in instance_fleets:
        if fleet['InstanceFleetType'] == 'TASK':
            emr_client.modify_instance_fleet(
                ClusterId=cluster.id,
                InstanceFleet={
                    'InstanceFleetId': fleet['Id'],
                    'TargetOnDemandCapacity': 0,
                    'TargetSpotCapacity': cluster.scaling_policy_max_units - cluster.scaling_policy_max_core_units
                }
            )
    return True