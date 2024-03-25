from sqlalchemy import Column, String, JSON, DateTime
from database import Base, engine


class Cluster(Base):
    __tablename__ = 'clusters'

    id = Column(String, primary_key=True)
    cluster_name = Column(String)
    cluster_group = Column(String)
    configuration = Column(JSON)
    last_scale_out_ts = Column(DateTime)
    last_scale_in_ts = Column(DateTime)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


Base.metadata.create_all(engine)
