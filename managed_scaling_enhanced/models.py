from datetime import datetime

from sqlalchemy import Column, String, JSON, DateTime, Integer, Float, Index
import pprint
from managed_scaling_enhanced.database import Base, engine
import requests
from managed_scaling_enhanced.utils import ec2_types


class Cluster(Base):
    __tablename__ = 'clusters'

    id = Column(String, primary_key=True)
    cluster_name = Column(String)
    cluster_group = Column(String)
    cpu_usage_upper_bound = Column(Float, default=0.6)
    cpu_usage_lower_bound = Column(Float, default=0.4)
    cpu_usage_period_minutes = Column(Float, default=15)
    cool_down_period_minutes = Column(Float, default=5)
    last_scale_in_ts = Column(DateTime)
    last_scale_out_ts = Column(DateTime)
    initial_managed_scaling_policy = Column(JSON)
    current_managed_scaling_policy = Column(JSON)
    instance_fleets = Column(JSON)
    instance_groups = Column(JSON)
    # instances_latest_ready_time = Column(DateTime)
    yarn_metrics = Column(JSON)
    cpu_usage = Column(Float)
    master_dns_name = Column(String)
    max_capacity_limit = Column(Integer)

    def to_dict(self):
        d = {
            'Cluster ID': self.id,
            'CPU usage lower bound': self.cpu_usage_lower_bound,
            'CPU usage upper bound': self.cpu_usage_upper_bound,
            'CPU usage': self.cpu_usage,
            'Spot capacity': self.current_task_spot_capacity,
            'OD capacity': self.current_task_od_capacity,
            'Initial max capacity': self.initial_max_units,
            'Current Max capacity': self.current_max_units,
            'Current Min capacity': self.current_min_units,
            'Max capacity limit': self.max_capacity_limit
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
    def task_instance_fleet(self):
        if self.instance_fleets:
            for fleet in self.instance_fleets:
                if fleet['InstanceFleetType'] == 'TASK':
                    return fleet

    # @property
    # def task_instance_fleet_status(self):
    #     if self.task_instance_fleet:
    #         return self.task_instance_fleet['Status']['State']

    @property
    def not_resizing(self):
        if self.is_fleet:
            return self.task_instance_fleet['Status']['State'] == 'RUNNING'
        else:
            for group in self.instance_groups:
                if group['Status']['State'] != 'RUNNING':
                    return False
            return True

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
    run_id = Column(Integer, index=True)
    action = Column(String)
    cluster_id = Column(Integer)
    event_time = Column(DateTime)
    data = Column(JSON)


class CpuUsage(Base):
    __tablename__ = 'cpu_usage'
    id = Column(Integer, primary_key=True, autoincrement=True)
    instance_id = Column(String)
    total_seconds = Column(Float)
    idle_seconds = Column(Float)
    event_time = Column(DateTime, index=True)

    __table_args__ = (
        Index('idx_instance_id_time', 'instance_id', 'event_time'),
    )

    @property
    def busy_time(self):
        return self.total_seconds - self.idle_seconds


class EMREvent(Base):
    __tablename__ = 'emr_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String)
    cluster_id = Column(String)
    source = Column(String)
    state = Column(String)
    message = Column(String)
    raw_message = Column(JSON)
    event_time = Column(DateTime, index=True)
    create_time = Column(DateTime, index=True)


Base.metadata.create_all(engine)
