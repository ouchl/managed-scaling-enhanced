import json

from sqlalchemy import Column, String, JSON, DateTime, Integer

from managed_scaling_enhanced.config import Config
from managed_scaling_enhanced.database import Base, engine
import orjson


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
        return Config(**self.configuration)

    def modify_scaling_policy(self, max_units=None, max_od_units=None):
        if max_units:
            self.managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits'] = max_units
        if max_od_units:
            self.managed_scaling_policy['ComputeLimits']['MaximumOnDemandCapacityUnits'] = max_od_units

    @property
    def scaling_policy_max_units(self):
        return self.managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits']

    @property
    def scaling_policy_min_units(self):
        return self.managed_scaling_policy['ComputeLimits']['MaximumCapacityUnits']

    @property
    def scaling_policy_max_od_units(self):
        return self.managed_scaling_policy['ComputeLimits']['MaximumOnDemandCapacityUnits']

    @property
    def scaling_policy_max_core_units(self):
        return self.managed_scaling_policy['ComputeLimits']['MaximumCoreCapacityUnits']

    def __repr__(self):
        return orjson.dumps(self.to_dict()).decode("utf-8")


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, index=True)
    action = Column(String)
    cluster_id = Column(Integer)
    event_time = Column(DateTime)
    data = Column(JSON)


Base.metadata.create_all(engine)
