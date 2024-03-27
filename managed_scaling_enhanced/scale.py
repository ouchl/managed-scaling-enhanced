from managed_scaling_enhanced.database import Session
from managed_scaling_enhanced.models import Cluster
from managed_scaling_enhanced.metrics import Metric
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
import boto3
import orjson


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

    def __repr__(self):
        return orjson.dumps(self.__dict__).decode("utf-8")


def get_scale_out_flags(cluster: Cluster, metrics: Metric):
    config = cluster.config_obj
    yarn_mem_percentage_flag = metrics.ScaleOutAvgYARNMemoryAvailablePercentage * 100 <= config.scaleOutAvgYARNMemoryAvailablePercentageValue
    free_mem_flag = metrics.ScaleOutAvgCapacityRemainingGB <= config.scaleOutAvgCapacityRemainingGBValue
    apps_pending_flag = metrics.ScaleOutAvgPendingAppNum >= config.scaleOutAvgPendingAppNumValue
    cpu_load_flag = metrics.ScaleOutAvgTaskNodeCPULoad >= config.scaleOutAvgTaskNodeCPULoadValue
    max_unit_flag = cluster.scaling_policy_max_units < config.maximumUnits
    overall_flag = ((yarn_mem_percentage_flag or free_mem_flag or apps_pending_flag)
                    and cpu_load_flag and max_unit_flag)
    scale_out_flags = ScaleFlags(
        YarnMemPercentageFlag=yarn_mem_percentage_flag,
        FreeMemFlag=free_mem_flag,
        AppsPendingFlag=apps_pending_flag,
        CpuLoadFlag=cpu_load_flag,
        MaxUnitFlag=max_unit_flag,
        OverallFlag=overall_flag
    )
    return scale_out_flags


def get_scale_in_flags(cluster: Cluster, metrics: Metric):
    config = cluster.config_obj
    yarn_mem_percentage_flag = metrics.ScaleInAvgYARNMemoryAvailablePercentage * 100 > config.scaleInAvgYARNMemoryAvailablePercentageValue
    free_mem_flag = metrics.ScaleInAvgCapacityRemainingGB > config.scaleInAvgCapacityRemainingGBValue
    apps_pending_flag = metrics.ScaleInAvgPendingAppNum < config.scaleInAvgPendingAppNumValue
    cpu_load_flag = metrics.ScaleInAvgTaskNodeCPULoad < config.scaleInAvgTaskNodeCPULoadValue
    max_unit_flag = cluster.scaling_policy_max_units > config.minimumUnits
    overall_flag = ((yarn_mem_percentage_flag or free_mem_flag or apps_pending_flag or cpu_load_flag)
                    and max_unit_flag)
    scale_in_flags = ScaleFlags(
        YarnMemPercentageFlag=yarn_mem_percentage_flag,
        FreeMemFlag=free_mem_flag,
        AppsPendingFlag=apps_pending_flag,
        CpuLoadFlag=cpu_load_flag,
        MaxUnitFlag=max_unit_flag,
        OverallFlag=overall_flag
    )
    return scale_in_flags


def scale_out(cluster: Cluster, metrics: Metric) -> bool:
    current_time = datetime.utcnow()
    config = cluster.config_obj
    last_scale_out_ts = cluster.last_scale_out_ts or datetime.min
    # 检查是否在冷却时间内
    if (current_time - last_scale_out_ts).total_seconds() < config.scaleOutCooldownSeconds:
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
        od_step = int(metrics.current_reserved_virtual_cores * config.scaleOutOnDemandFactor)
    else:
        step = int(
            (metrics.current_total_virtual_cores / metrics.current_apps_running) * config.scaleOutFactor
        )
        od_step = int(
            (metrics.current_total_virtual_cores / metrics.current_apps_running) * config.scaleOutOnDemandFactor
        )
    new_max_capacity_units = cluster.scaling_policy_max_units + step

    # 确保新的 MaximumCapacityUnits 大于 MinimumCapacityUnits
    new_max_capacity_units = max(new_max_capacity_units, cluster.scaling_policy_min_units + 1)

    # 确保新的 MaximumCapacityUnits 不超过最大限制
    new_max_capacity_units = min(new_max_capacity_units, config.maximumUnits)

    new_max_od_capacity_units = cluster.scaling_policy_max_od_units
    # 如果不设置spot switch按系数scale out od
    if not config.spotSwitchOnDemand:
        new_max_od_capacity_units += od_step

        # 确保新的 MaximumOnDemandCapacityUnits 不超过最大限制
        new_max_od_capacity_units = min(new_max_od_capacity_units, config.maximumOnDemandUnits)
    else:
        timeout_timestamp = datetime.utcnow() - timedelta(seconds=config.spotInstancesTimeout)
        with Session() as session:
            result = session.execute("""
select min(json_extract(data, '$.managed_scaling_policy.ComputeLimits.MaximumCapacityUnits'))
from events
where action='GetCluster' and cluster_id= :id and run_id >= :ts
            """, {'id': cluster.id, 'ts': int(timeout_timestamp.timestamp())})

            min_max_capacity_units = result[0][0]

        if min_max_capacity_units > metrics.current_total_virtual_cores:
            # 需要补充 On-Demand 实例
            on_demand_units_to_add = min_max_capacity_units - metrics.current_total_virtual_cores
            new_max_od_capacity_units = cluster.scaling_policy_max_od_units + on_demand_units_to_add
            cluster.modify_scaling_policy(max_od_units=new_max_od_capacity_units)
            emr_client.put_managed_scaling_policy(cluster.id, cluster.managed_scaling_policy)

    cluster.modify_scaling_policy(max_units=new_max_capacity_units, max_od_units=new_max_od_capacity_units)
    # 应用新策略
    emr_client.put_managed_scaling_policy(cluster.id, cluster.managed_scaling_policy)
    cluster.last_scale_out_ts = current_time

    return True


def scale_in(cluster: Cluster, metrics: Metric) -> bool:
    current_time = datetime.utcnow()
    config = cluster.config_obj
    last_scale_in_ts = cluster.last_scale_in_ts or datetime.min
    if (current_time - last_scale_in_ts).total_seconds() < config.scaleInCooldownSeconds:
        logger.info(
            f"⌛️ Skipping scale in operation due to cooldown period ({config.scaleInCooldownSeconds} seconds).")
        return False

    new_max_od_units = cluster.scaling_policy_max_od_units
    new_max_capacity_units = cluster.scaling_policy_max_units
    # max_od_units = max(cluster.scaling_policy_max_core_units, cluster.scaling_policy_max_od_units)
    if metrics.current_apps_pending == 0:
        logger.info("No pending applications, setting minimum capacity.")
        new_max_capacity_units = config.minimumUnits

    # 如果 apps_pending 不为 0
    else:
        # 计算新的 MaximumCapacityUnits
        new_max_capacity_units = max(config.minimumUnits, cluster.scaling_policy_max_units - int(
            (metrics.current_total_virtual_cores / metrics.current_apps_running) * config.scaleInFactor))

    instance_fleets = emr_client.list_instance_fleets(ClusterId=cluster.id)['InstanceFleets']

    if not config.spotSwitchOnDemand:
        if metrics.current_apps_pending == 0:
            new_max_od_units = cluster.scaling_policy_max_core_units
        # 如果 apps_pending 不为 0
        else:
            # 计算新的 MaximumCapacityUnits
            new_max_od_units = max(cluster.scaling_policy_max_core_units, cluster.scaling_policy_max_units - int(
                (metrics.current_total_virtual_cores / metrics.current_apps_running) * config.scaleInOnDemandFactor))

        # 修改 Instance Fleets
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
    cluster.modify_scaling_policy(max_units=new_max_capacity_units, max_od_units=new_max_od_units)
    # 应用新策略
    emr_client.put_managed_scaling_policy(cluster.id, cluster.managed_scaling_policy)
    cluster.last_scale_in_ts = current_time
    return True


if __name__ == '__main__':
    logger.setLevel('INFO')
    # from managed_scaling_enhanced.metrics import get_metrics
    # session = Session()
    # cluster = session.get(Cluster, 'j-1SJOW088JSHLK')
    # metrics = get_metrics(cluster)
    # scale_out(cluster, metrics)
    # cluster.last_scale_out_ts = datetime.utcnow()
    # session.commit()
