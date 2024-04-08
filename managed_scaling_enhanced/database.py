from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import orjson

# Create an engine
engine = create_engine('sqlite:///sqlite.db', echo=False,
                       json_serializer=lambda x: orjson.dumps(x).decode('utf8'),
                       json_deserializer=lambda x: orjson.loads(x))

# Create a base class for our declarative class definitions
Base = declarative_base()

# Create a sessionmaker
Session = sessionmaker(bind=engine)
