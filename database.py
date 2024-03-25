from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create an engine
engine = create_engine('sqlite:///data.db', echo=False)

# Create a base class for our declarative class definitions
Base = declarative_base()

# Create a sessionmaker
Session = sessionmaker(bind=engine)
