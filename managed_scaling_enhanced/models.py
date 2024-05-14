from datetime import datetime

import enum
from sqlalchemy import Column, String, JSON, DateTime, Integer, Float, Index, Boolean, Text, BigInteger, Enum
import pprint
from managed_scaling_enhanced.database import Base, engine
import requests
from managed_scaling_enhanced.utils import ec2_types


class ResizePolicy(enum.Enum):
    CPU_BASED = 'CPU_BASED'
    RESOURCE_BASED = 'RESOURCE_BASED'


class Cluster(Base):
    __tablename__ = 'clusters'

    id = Column(String(20), primary_key=True)
    cluster_name = Column(Text)
    cluster_group = Column(String(20))
    cpu_usage_upper_bound = Column(Float, default=0.6)
    cpu_usage_lower_bound = Column(Float, default=0.4)
    metrics_lookback_period_minutes = Column(Float, default=15)
    cool_down_period_minutes = Column(Float, default=5)
    last_scale_in_ts = Column(DateTime, default=datetime.min)
    last_scale_out_ts = Column(DateTime, default=datetime.min)
    initial_managed_scaling_policy = Column(JSON)
    current_managed_scaling_policy = Column(JSON)
    instance_fleets = Column(JSON)
    instance_groups = Column(JSON)
    master_dns_name = Column(String(100))
    max_capacity_limit = Column(Integer)
    scale_in_factor = Column(Float, default=1)
    scale_out_factor = Column(Float, default=1)
    active = Column(Boolean, default=True)
    resize_policy = Column(Enum(ResizePolicy), default=ResizePolicy.CPU_BASED)

    def to_dict(self):
        d = {
            'Cluster ID': self.id,
            'CPU usage lower bound': self.cpu_usage_lower_bound,
            'CPU usage upper bound': self.cpu_usage_upper_bound,
            'Spot capacity': self.current_task_spot_capacity,
            'OD capacity': self.current_task_od_capacity,
            'Initial max capacity': self.initial_max_units,
            'Current Max capacity': self.current_max_units,
            'Current Min capacity': self.current_min_units,
            'Max capacity limit': self.max_capacity_limit,
            'Scale in factor': self.scale_in_factor,
            'Scale out factor': self.scale_out_factor,
            'Resize policy': str(self.resize_policy)
        }
        return d

    @property
    def initial_min_units(self):
        return self.initial_managed_scaling_policy['ComputeLimits']['MinimumCapacityUnits']

    @property
    def initial_max_units(self):
        return self.initial_managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits']

    @property
    def initial_max_core_units(self):
        return self.initial_managed_scaling_policy['ComputeLimits']['MaximumCoreCapacityUnits']

    @property
    def current_min_units(self):
        if self.current_managed_scaling_policy:
            return self.current_managed_scaling_policy['ComputeLimits']['MinimumCapacityUnits']

    @property
    def current_max_units(self):
        if self.current_managed_scaling_policy:
            return self.current_managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits']

    @property
    def current_max_core_units(self):
        if self.current_managed_scaling_policy:
            return self.current_managed_scaling_policy['ComputeLimits']['MaximumCoreCapacityUnits']

    @property
    def current_max_od_units(self):
        if self.current_managed_scaling_policy:
            return self.current_managed_scaling_policy['ComputeLimits']['MaximumOnDemandCapacityUnits']

    @property
    def task_instance_fleet(self):
        if self.instance_fleets:
            for fleet in self.instance_fleets:
                if fleet['InstanceFleetType'] == 'TASK':
                    return fleet

    @property
    def core_instance_fleet(self):
        if self.instance_fleets:
            for fleet in self.instance_fleets:
                if fleet['InstanceFleetType'] == 'CORE':
                    return fleet

    @property
    def is_resizing(self):
        flag = False
        if self.is_fleet:
            for fleet in self.instance_fleets:
                if fleet['Status']['State'] != 'RUNNING':
                    flag = True
        else:
            for group in self.instance_groups:
                if group['Status']['State'] != 'RUNNING':
                    flag = True
        return flag

    @property
    def task_target_od_capacity(self):
        if self.task_instance_fleet:
            return self.task_instance_fleet['TargetOnDemandCapacity']

    @property
    def task_target_spot_capacity(self):
        if self.task_instance_fleet:
            return self.task_instance_fleet['TargetSpotCapacity']

    def modify_scaling_policy(self, max_units=None, max_od_units=None):
        if max_units:
            self.current_managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits'] = max_units
        if max_od_units:
            self.current_managed_scaling_policy['ComputeLimits']['MaximumOnDemandCapacityUnits'] = max_od_units

    def get_info_str(self):
        d = {}
        for k, v in self.to_dict().items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
            elif k not in ['yarn_metrics']:
                d[k] = v
        return pprint.pformat(d)

    def kill_app(self, app_id):
        return requests.put(f"http://{self.master_dns_name}:8088/ws/v1/cluster/apps/{app_id}/state", json={'state': 'KILLED'}, timeout=5)

    def list_running_apps(self):
        r = requests.get(f"http://{self.master_dns_name}:8088/ws/v1/cluster/apps?states=RUNNING", timeout=5).json()
        app_ids = []
        if r['apps']:
            for app in r['apps']['app']:
                app_ids.append(app['id'])
        return app_ids

    @property
    def managed_scaling_unit_type(self):
        return self.current_managed_scaling_policy['ComputeLimits']['UnitType']

    @property
    def is_fleet(self):
        return self.current_managed_scaling_policy['ComputeLimits']['UnitType'] == 'InstanceFleetUnits'

    @property
    def current_task_spot_capacity(self):
        if self.managed_scaling_unit_type == 'InstanceFleetUnits':
            return self.task_target_spot_capacity
        elif self.managed_scaling_unit_type == 'Instances':
            instances = 0
            for group in self.task_instance_groups:
                if group['Market'] == 'SPOT':
                    instances += group['RunningInstanceCount']
            return instances
        else:
            count = 0
            for group in self.task_instance_groups:
                if group['Market'] == 'SPOT':
                    count += group['RunningInstanceCount'] * ec2_types[group['InstanceType']]
            return count

    @property
    def task_instance_groups(self):
        if self.instance_groups:
            return [group for group in self.instance_groups if group['InstanceGroupType'] == 'TASK']

    @property
    def current_task_od_capacity(self):
        if self.managed_scaling_unit_type == 'InstanceFleetUnits':
            return self.task_target_od_capacity
        elif self.managed_scaling_unit_type == 'Instances':
            instances = 0
            for group in self.task_instance_groups:
                if group['Market'] == 'ON_DEMAND':
                    instances += group['RunningInstanceCount']
            return instances
        else:
            count = 0
            for group in self.task_instance_groups:
                if group['Market'] == 'ON_DEMAND':
                    count += group['RunningInstanceCount'] * ec2_types[group['InstanceType']]
            return count

    @property
    def current_task_total_capacity(self):
        return self.current_task_spot_capacity + self.current_task_od_capacity


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String(255))
    cluster_id = Column(String(20))
    event_time = Column(DateTime, index=True)
    current_max_units = Column(Integer)
    target_max_units = Column(Integer)
    is_resizing = Column(Boolean)
    is_cooling_down = Column(Boolean)
    data = Column(JSON)

    __table_args__ = (
        Index('idx_event_cluster_id_time', 'cluster_id', 'event_time'),
    )


class CpuUsage(Base):
    __tablename__ = 'cpu_usage'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(String(20))
    instance_id = Column(String(20))
    total_seconds = Column(Float)
    idle_seconds = Column(Float)
    event_time = Column(DateTime, index=True)

    __table_args__ = (
        Index('idx_cpu_usage_cluster_instance_time', 'cluster_id', 'instance_id', 'event_time'),
    )

    @property
    def busy_seconds(self):
        return self.total_seconds - self.idle_seconds


class EMREvent(Base):
    __tablename__ = 'emr_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String(20))
    cluster_id = Column(String(20))
    source = Column(String(20))
    state = Column(String(20))
    message = Column(Text)
    raw_message = Column(JSON)
    event_time = Column(DateTime, index=True)
    create_time = Column(DateTime, index=True)


class Metric(Base):
    __tablename__ = 'metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(String(20))
    yarn_app_running = Column(Integer)
    yarn_app_pending = Column(Integer)

    yarn_reserved_mem = Column(Integer)
    yarn_pending_mem = Column(Integer)
    yarn_allocated_mem = Column(Integer)
    yarn_available_mem = Column(Integer)
    yarn_total_mem = Column(Integer)

    yarn_reserved_vcore = Column(Integer)
    yarn_pending_vcore = Column(Integer)
    yarn_allocated_vcore = Column(Integer)
    yarn_available_vcore = Column(Integer)
    yarn_total_vcore = Column(Integer)

    yarn_active_nodes = Column(Integer)

    total_cpu_seconds = Column(BigInteger)
    idle_cpu_seconds = Column(BigInteger)

    event_time = Column(DateTime, index=True)

    __table_args__ = (
        Index('idx_metrics_cluster_id_time', 'cluster_id', 'event_time'),
    )


class AvgMetric(Base):
    __tablename__ = 'avg_metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(String(20))
    lookback_period = Column(Integer)
    yarn_app_running = Column(Integer)
    yarn_app_pending = Column(Integer)

    yarn_reserved_mem = Column(Integer)
    yarn_pending_mem = Column(Integer)
    yarn_allocated_mem = Column(Integer)
    yarn_available_mem = Column(Integer)
    yarn_total_mem = Column(Integer)

    yarn_reserved_vcore = Column(Integer)
    yarn_pending_vcore = Column(Integer)
    yarn_allocated_vcore = Column(Integer)
    yarn_available_vcore = Column(Integer)
    yarn_total_vcore = Column(Integer)

    yarn_active_nodes = Column(Integer)

    cpu_utilization = Column(Float)

    event_time = Column(DateTime, index=True)

    __table_args__ = (
        Index('idx_avg_metrics_cluster_id_time', 'cluster_id', 'event_time'),
    )


Base.metadata.create_all(engine)
