from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base

#Define database connection 
DATABASE_URL = "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/sailors_db_orm"

engine = create_engine(DATABASE_URL, echo=True) #Connect to existing database
Base = declarative_base() #Base SQLAlchemy class
SessionLocal = sessionmaker(bind=engine)

#Models
class Sailor(Base):
    __tablename__ = "sailors"

    sid = Column(Integer, primary_key = True)
    sname = Column(String(30))
    rating = Column(Integer)
    age = Column(Integer)

class Boat(Base):
    __tablename__ = "boats"

    bid = Column(Integer, primary_key = True)
    bname = Column(String(20))
    color = Column(String(20))
    length = Column(Integer)

class Reserve(Base):
    __tablename__ = "reserves"

    #ref foreign keys so only existing sid and bid are allowed 
    sid = Column(Integer, ForeignKey("sailors.sid"), primary_key = True) 
    bid = Column(Integer, ForeignKey("boats.bid"), primary_key = True)
    day = Column(Date, primary_key = True)

def init_db():
    """
    Initialize db
    """
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    init_db()

