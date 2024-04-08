from sqlalchemy import Column, String, JSON, DateTime, Integer, Float

from managed_scaling_enhanced.database import Base, engine


class Cluster(Base):
    __tablename__ = 'clusters'

    id = Column(String, primary_key=True)
    cluster_name = Column(String)
    cluster_group = Column(String)
    cpu_usage_upper_bound = Column(Float, default=0.8)
    cpu_usage_lower_bound = Column(Float, default=0.4)
    cpu_usage_period_minutes = Column(Float, default=15)
    cool_down_period_minutes = Column(Float, default=5)
    last_scale_in_ts = Column(DateTime)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, index=True)
    action = Column(String)
    cluster_id = Column(Integer)
    event_time = Column(DateTime)
    data = Column(JSON)


Base.metadata.create_all(engine)
