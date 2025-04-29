from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ARRAY
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# Setup engine
DATABASE_URL = "postgresql+psycopg2://postgres:mysecretpassword@localhost:5433/government_databases"
engine = create_engine(DATABASE_URL)

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    title = Column(Text, nullable=True)
    organization_type = Column(Text, nullable=True)
    formats = Column(ARRAY(Text), nullable=False)
    tags = Column(ARRAY(Text), nullable=False)
    publisher_heading = Column(Text, nullable=True)
    publisher = Column(Text, nullable=True)
    # date_created = Column(TIMESTAMP, nullable=True)
    date_last_updated = Column(TIMESTAMP, nullable=True)
    url = Column(Text, nullable=False, unique=True)

if __name__ == "__main__":
    Base.metadata.create_all(engine)

