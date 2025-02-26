from sqlalchemy import (
    create_engine,
    Column, 
    Integer, 
    String, 
    Date, 
    ForeignKey, 
    UniqueConstraint, 
    case, 
    select
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from datetime import date

#Define database connection 
DATABASE_URL = "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/extended_sailors_db_orm"

engine = create_engine(DATABASE_URL, echo=True) #Connect to existing database
Base = declarative_base() #Base SQLAlchemy class to make tables and such
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

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

    __table_args__ = (
        UniqueConstraint("bid","day", name="uq_boat_day"),
    )

def init_db():
    """
    Initialize db
    """
    Base.metadata.create_all(engine)

def populate_data():
    """Populates the database with sample data"""

        # Insert Sailors
    sailors = [
        Sailor(sid=22, sname="dusting", rating=7, age=45),
        Sailor(sid=29, sname="brutus", rating=1, age=33),
        Sailor(sid=31, sname="lubber", rating=8, age=55),
        Sailor(sid=32, sname="andy", rating=8, age=25),
        Sailor(sid=58, sname="rusty", rating=10, age=35),
        Sailor(sid=64, sname="horatio", rating=7, age=16),
        Sailor(sid=71, sname="zorba", rating=10, age=35),
        Sailor(sid=74, sname="horatio", rating=9, age=25),
        Sailor(sid=85, sname="art", rating=3, age=25),
        Sailor(sid=95, sname="bob", rating=3, age=63),
        Sailor(sid=23, sname="emilio", rating=7, age=45),
        Sailor(sid=24, sname="scruntus", rating=1, age=33),
        Sailor(sid=35, sname="figaro", rating=8, age=55),
        Sailor(sid=59, sname="stum", rating=8, age=25),
        Sailor(sid=60, sname="jit", rating=10, age=35),
        Sailor(sid=61, sname="ossola", rating=7, age=16),
        Sailor(sid=62, sname="shaun", rating=10, age=35),
        Sailor(sid=88, sname="dan", rating=9, age=25),
        Sailor(sid=89, sname="dye", rating=3, age=25),
        Sailor(sid=90, sname="vin", rating=3, age=63),
    ]
    
    # Insert Boats
    boats = [
        Boat(bid=101, bname="Interlake", color="blue", length=45),
        Boat(bid=102, bname="Interlake", color="red", length=45),
        Boat(bid=103, bname="Clipper", color="green", length=40),
        Boat(bid=104, bname="Clipper", color="red", length=40),
        Boat(bid=105, bname="Marine", color="red", length=35),
        Boat(bid=106, bname="Marine", color="green", length=35),
        Boat(bid=107, bname="Marine", color="blue", length=35),
        Boat(bid=108, bname="Driftwood", color="red", length=35),
        Boat(bid=109, bname="Driftwood", color="blue", length=35),
        Boat(bid=110, bname="Klapser", color="red", length=30),
        Boat(bid=111, bname="Sooney", color="green", length=28),
        Boat(bid=112, bname="Sooney", color="red", length=28),
    ]
    
    # Insert Reservations
    reservations = [
        Reserve(sid=23, bid=104, day=date(1998, 10, 10)),
        # Reserve(sid=24, bid=104, day=date(1998, 10, 10)),
        Reserve(sid=35, bid=104, day=date(1998, 8, 10)),
        Reserve(sid=59, bid=105, day=date(1998, 7, 10)),
        Reserve(sid=23, bid=105, day=date(1998, 11, 10)),
        Reserve(sid=35, bid=105, day=date(1998, 11, 6)),
        Reserve(sid=59, bid=106, day=date(1998, 11, 12)),
        Reserve(sid=60, bid=106, day=date(1998, 9, 5)),
        Reserve(sid=60, bid=106, day=date(1998, 9, 8)),
        Reserve(sid=88, bid=107, day=date(1998, 9, 8)),
        Reserve(sid=89, bid=108, day=date(1998, 10, 10)),
        Reserve(sid=90, bid=109, day=date(1998, 10, 10)),
        Reserve(sid=89, bid=109, day=date(1998, 8, 10)),
        Reserve(sid=60, bid=109, day=date(1998, 7, 10)),
        Reserve(sid=59, bid=109, day=date(1998, 11, 10)),
        Reserve(sid=62, bid=110, day=date(1998, 11, 6)),
        Reserve(sid=88, bid=110, day=date(1998, 11, 12)),
        Reserve(sid=88, bid=110, day=date(1998, 9, 5)),
        Reserve(sid=88, bid=111, day=date(1998, 9, 8)),
        Reserve(sid=61, bid=112, day=date(1998, 9, 8)),
        Reserve(sid=22, bid=101, day=date(1998, 10, 10)),
        Reserve(sid=22, bid=102, day=date(1998, 10, 10)),
        Reserve(sid=22, bid=103, day=date(1998, 8, 10)),
        Reserve(sid=22, bid=104, day=date(1998, 7, 10)),
        Reserve(sid=31, bid=102, day=date(1998, 11, 10)),
        Reserve(sid=31, bid=103, day=date(1998, 11, 6)),
        Reserve(sid=31, bid=104, day=date(1998, 11, 12)),
        Reserve(sid=64, bid=101, day=date(1998, 9, 5)),
        Reserve(sid=64, bid=102, day=date(1998, 9, 8)),
        Reserve(sid=74, bid=103, day=date(1998, 9, 8)),
    ]

    #Add to session and commit 
    session.bulk_save_objects(sailors)
    session.bulk_save_objects(boats)
    session.bulk_save_objects(reservations)
    session.commit()
    print("Data sucessfully inserted")

def reserve_boat(session, sid, bid, day):
    """
    Attempt to insert a new reservation while checking for double-booking.
    """

    try: 
        new_reservation = Reserve(sid=sid, bid=bid, day=day)
        session.add(new_reservation)
        session.commit()
        print(f"Reservation added for Sailor {sid} on Boat {bid} on {day}.")
        return True
    except IntegrityError:
        session.rollback()
        print(f"Reservation FAILED: Another sailor already reserved Boat {bid} on {day}!")
        return False
    
def delete_reservation(session, sid, bid, day):
    """
    Delete a reservation from the database 
    Could be used in terms of cancelled reservation
    """

    try:
        reservation = session.query(Reserve).filter_by(sid=sid, bid=bid, day=day).first()
        if reservation:
            session.delete(reservation)
            session.commit()
            print(f"Reservation deleted for Sailor {sid} on Boat{bid} on {day}")
            return True
        
        else:
            print(f"No reservation found for Sailor {sid} on Boat {bid} on day {day}.")
            return False
    except Exception as e:
        session.rollback()
        print(f"Error deleting reservations. Please try again: {e}")
        return False
    

def get_daily_inventory(test_db, report_date):
    """
    Generates a report of boat availability for a given date
    """

    inventory_report = (
        test_db.query(
            Boat.bid, 
            Boat.bname,
            case(
                [(Reserve.bid.isnot(None), "Reserved")],
                else_ = "Available"
            ).label("status")
        )
        .outerjoin(Reserve, (Boat.bid == Reserve.bid) & Reserve.day == report_date)
        .order_by(Boat.bid)
        .all()
    )
    
    # Print Report
    print(f"Inventory Report for {report_date}:")
    for row in inventory_report:
        print(row)

    return inventory_report


if __name__ == "__main__":
    init_db()
    populate_data()

    report_date = date(1998, 11, 12)

    _ = get_daily_inventory(session, report_date)
