import json

from sqlalchemy import Column, String, JSON, DateTime, Integer

from config import Config
from database import Base, engine


class Cluster(Base):
    __tablename__ = 'clusters'

    id = Column(String, primary_key=True)
    cluster_name = Column(String)
    cluster_group = Column(String)
    configuration = Column(JSON)
    last_scale_out_ts = Column(DateTime)
    last_scale_in_ts = Column(DateTime)
    managed_scaling_policy = Column(JSON)
    cluster_info = Column(JSON)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @property
    def config_obj(self):
        return Config(**json.loads(self.configuration))

    @property
    def cluster_info_obj(self):
        return json.loads(self.cluster_info)

    @property
    def managed_scaling_policy_obj(self):
        return json.loads(self.managed_scaling_policy)

    def modify_scaling_policy(self, max_units, max_od_units=None):
        policy = self.managed_scaling_policy_obj
        policy['MaximumCapacityUnits'] = max_units
        if max_od_units:
            policy['MaximumOnDemandCapacityUnits'] = max_od_units
        self.managed_scaling_policy = json.dumps(policy)

    @property
    def scaling_policy_max_units(self):
        return self.managed_scaling_policy_obj['ComputeLimits']['MaximumCapacityUnits']

    @property
    def scaling_policy_min_units(self):
        return self.managed_scaling_policy_obj['ComputeLimits']['MaximumCapacityUnits']

    @property
    def scaling_policy_max_od_units(self):
        return self.managed_scaling_policy_obj['ComputeLimits']['MaximumOnDemandCapacityUnits']

    @property
    def scaling_policy_max_core_units(self):
        return self.managed_scaling_policy_obj['ComputeLimits']['MaximumCoreCapacityUnits']


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer)
    action = Column(String)
    cluster_id = Column(Integer)
    event_time = Column(DateTime)
    data = Column(String)


Base.metadata.create_all(engine)
