import pytest
from sqlalchemy import func, create_engine, text, select, not_, exists
from sqlalchemy.orm import sessionmaker
from create_sailors_orm import Base, Sailor, Boat, Reserve, engine as orm_engine
from datetime import date

# Connect to the original `sailors_db` populated by sailors-mysql.sql
raw_db_engine = create_engine("postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/sailors_db")
RawSessionLocal = sessionmaker(bind=raw_db_engine)

@pytest.fixture(scope="function")
def raw_db():
    """Connect to original database populaed by sailors-mysql.sql"""
    session = RawSessionLocal()
    yield session
    session.close()


@pytest.fixture(scope="function")
def test_db():
    """Connect to the ORM-based test database."""
    session = sessionmaker(bind=orm_engine)()
    yield session
    session.close()

def test_1_number_of_boat_reservations_no_unreserved_boats(raw_db, test_db):
    """
    List, for every boat, the number of times it has been reserved, excluding those boats that 
    have never been reserved (list the id and the name)
    """

    raw_sql = text("""
                SELECT boats.bname, reserves.bid, COUNT(*) AS reservation_count
                FROM reserves
                JOIN boats ON reserves.bid = boats.bid
                GROUP BY reserves.bid, boats.bname
                ORDER BY reservation_count DESC;
                """)
    # raw_results = test_db.execute(raw_sql).fetchall()
    raw_results = raw_db.execute(raw_sql).fetchall()


    # ORM Query
    orm_results = (
        test_db.query(Boat.bname, Reserve.bid, func.count(Reserve.sid).label("reservation_count"))
        .join(Reserve, Boat.bid == Reserve.bid)
        .group_by(Reserve.bid, Boat.bname)
        .order_by(func.count(Reserve.sid).desc())
        .all()
    )

    # Convert to list of tuples for direct comparision
    raw_results = [(row[0].strip(), row[1], row[2]) for row in raw_results] #strip empty space in bname 
    orm_results = [(row[0], row[1], row[2]) for row in orm_results]

    print("\nORM Results:")
    for row in orm_results:
        print(row)

    print("\nRaw SQL Results:")
    for row in raw_results:
        print(row)

    assert orm_results == raw_results, "ORM and raw SQL results do not match!"

def test_2_sailors_who_have_reserved_every_red_boat(raw_db, test_db):
    """
    List those sailors who have reserved every red boat (list the id and the name)
    """

    raw_sql = text("""
        SELECT sailors.sid, sailors.sname
        FROM sailors
        WHERE NOT EXISTS (
            SELECT boats.bid
            FROM boats
            WHERE boats.color = 'red'
            EXCEPT
            SELECT reserves.bid
            FROM reserves
            WHERE reserves.sid = sailors.sid
        );
    """)

    raw_results = raw_db.execute(raw_sql).fetchall()

    #Subqueries
    red_boats = select(Boat.bid).where(Boat.color=="red")
    reserved_boats_per_sailor = select(Reserve.bid).where(Reserve.sid == Sailor.sid)

    orm_results = (
        test_db.query(Sailor.sid, Sailor.sname)
        .filter(
            ~(red_boats.except_(reserved_boats_per_sailor).exists()
            )
            )
            .all()
    )

    raw_results = [(row[0], row[1].strip()) for row in raw_results] #strip empty space in sname 
    orm_results = [(row[0], row[1]) for row in orm_results]

    print("\nORM Results:")
    for row in orm_results:
        print(row)

    print("\nRaw SQL Results:")
    for row in raw_results:
        print(row)

    assert orm_results == raw_results, "ORM and raw SQL results do not match!"
