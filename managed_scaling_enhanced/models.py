from datetime import datetime

from sqlalchemy import Column, String, JSON, DateTime, Integer, Float
import pprint
from managed_scaling_enhanced.database import Base, engine


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
    task_fleet_latest_ready_time = Column(DateTime)
    total_used_resource = Column(Float)
    total_cpu_count = Column(Integer)
    task_used_resource = Column(Float)
    task_cpu_count = Column(Integer)
    yarn_apps_pending = Column(Integer)
    yarn_total_virtual_cores = Column(Integer)
    yarn_apps_running = Column(Integer)
    yarn_reserved_virtual_cores = Column(Integer)
    yarn_total_memory_mb = Column(Integer)
    yarn_available_memory_mb = Column(Integer)
    yarn_containers_pending = Column(Integer)

    def to_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name != 'instance_fleets'}
        d['task_target_od_capacity'] = self.task_target_od_capacity
        d['task_target_spot_capacity'] = self.task_target_spot_capacity
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
        return self.current_managed_scaling_policy['ComputeLimits']['MinimumCapacityUnits']

    @property
    def current_max_units(self):
        return self.current_managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits']

    @property
    def current_max_core_units(self):
        return self.current_managed_scaling_policy['ComputeLimits']['MaximumCoreCapacityUnits']

    @property
    def task_instance_fleet(self):
        if self.instance_fleets:
            for fleet in self.instance_fleets:
                if fleet['InstanceFleetType'] == 'TASK':
                    return fleet

    @property
    def task_instance_fleet_status(self):
        if self.task_instance_fleet:
            return self.task_instance_fleet['Status']['State']

    # @property
    # def task_instance_fleet_ready_time(self):
    #     if self.task_instance_fleet:
    #         return self.task_instance_fleet['Status']['Timeline']['ReadyDateTime']
    #
    # @property
    # def task_instance_fleet_ready_time_utc(self):
    #     if self.task_instance_fleet_ready_time:
    #         return self.task_instance_fleet_ready_time.astimezone(timezone.utc).replace(tzinfo=None)

    @property
    def task_target_od_capacity(self):
        if self.task_instance_fleet:
            return self.task_instance_fleet['TargetOnDemandCapacity']

    @property
    def task_target_spot_capacity(self):
        if self.task_instance_fleet:
            return self.task_instance_fleet['TargetSpotCapacity']

    @property
    def avg_task_cpu_usage(self):
        return self.task_used_resource/self.task_cpu_count

    @property
    def avg_total_cpu_usage(self):
        return self.total_used_resource / self.total_cpu_count

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
            else:
                d[k] = v
        return pprint.pformat(d)


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, index=True)
    action = Column(String)
    cluster_id = Column(Integer)
    event_time = Column(DateTime)
    data = Column(JSON)


Base.metadata.create_all(engine)
