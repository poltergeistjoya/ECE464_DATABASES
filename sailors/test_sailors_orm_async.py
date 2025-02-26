import pytest
from sqlalchemy import func, text, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sailors.orm.create_sailors_orm import Base, Sailor, Boat, Reserve, engine as orm_engine
from datetime import date

# Connect to the original `sailors_db` populated by sailors-mysql.sql
raw_db_engine = create_async_engine("postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/sailors_db")
RawSessionLocal = sessionmaker(bind=raw_db_engine, echo = True, future = True)


# Connect to orm `sailors_db_orm` populated by `create_sailors_orm.py`
ORM_URL = "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/sailors_db_orm"
test_db_engine = create_async_engine(ORM_URL, echo=True, future = True) #Connect to existing database

@pytest.fixture(scope="function")
async def raw_db():
    """Connect to original database populaed by sailors-mysql.sql"""
    # session = RawSessionLocal()
    # yield session
    # session.close()
    AsyncSessionLocal = sessionmaker(
    bind=raw_db_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
    async with AsyncSessionLocal() as session:
        yield session


@pytest.fixture(scope="function")
async def test_db():
    """Connect to the ORM-based test database."""
    # session = sessionmaker(bind=orm_engine)()
    # yield session
    # session.close()

    AsyncSessionLocal = sessionmaker(
    bind=test_db_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
    async with AsyncSessionLocal() as session:
        yield session


@pytest.mark.asyncio()
async def test_1_number_of_boat_reservations_no_unreserved_boats(raw_db, test_db):
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
    raw_results = await raw_db.execute(raw_sql).fetchall()


    # ORM Query
    orm_results = (
        await test_db.query(Boat.bname, Reserve.bid, func.count(Reserve.sid).label("reservation_count"))
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

@pytest.mark.asyncio()
async def test_2_sailors_who_have_reserved_every_red_boat(raw_db, test_db):
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

    raw_results = await raw_db.execute(raw_sql).fetchall()

    #Subqueries
    red_boats = select(Boat.bid).where(Boat.color=="red")
    reserved_boats_per_sailor = select(Reserve.bid).where(Reserve.sid == Sailor.sid)

    orm_results = await (
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

@pytest.mark.asyncio()
async def test_3_sailors_reserve_only_red_boats(raw_db, test_db):
    """
    List those sailors who have reserved only red boats
    """
    raw_sql = text("""
    SELECT s.sid, s.sname
    FROM sailors s
    WHERE EXISTS (
        SELECT 1 FROM reserves r WHERE r.sid = s.sid
    )
    AND NOT EXISTS (
        SELECT r.bid
        FROM reserves r
        JOIN boats b ON r.bid = b.bid
        WHERE r.sid = s.sid AND b.color <> 'red'
    );
    """)

    pass


@pytest.mark.asyncio()
async def test_4_boat_with_most_reservations(raw_db, test_db):
    """For which boat are there the most reservations?"""
    raw_sql = text("""
    WITH boat_reservations AS (
        SELECT boats.bname, reserves.bid, COUNT(*) AS reservation_count
        FROM reserves
        JOIN boats ON reserves.bid = boats.bid
        GROUP BY reserves.bid, boats.bname
        ORDER BY reservation_count
    )
    SELECT * FROM boat_reservations b
    WHERE b.reservation_count = (SELECT MAX(reservation_count) FROM boat_reservations);
    """)

    pass
    
@pytest.mark.asyncio()
async def test_5_sailors_never_reserve_red_boat(raw_db, test_db):
    """
    Select all sailors who have never reserved a red boat
    """
    raw_sql = text("""
    SELECT s.sid, s.sname
    FROM sailors s
    WHERE NOT EXISTS (
        SELECT r.bid
        FROM reserves r
        JOIN boats b ON r.bid = b.bid
        WHERE r.sid = s.sid AND b.color = 'red'
    );
    """)

    pass

@pytest.mark.asyncio()
async def test_6_avg_age_sailors_rating_10(raw_db, test_db):
    """
    Find the average age of sailors with a rating of 10
    """
    raw_sql = text("""
    WITH rating_10 AS (
        SELECT s.sid, s.rating, s.age
        FROM sailors s
        WHERE s.rating = 10
    )
    SELECT AVG(age) AS average_age FROM rating_10;
    """)

    pass

@pytest.mark.asyncio()
async def test_7_youngest_sailor_for_each_rating(raw_db, test_db):
    """
    For each rating find the name and the id of the youngest sailor
    Using query 7e to get only one sailor if two sailors have the youngest age. 
    """
    raw_sql = text("""
    SELECT DISTINCT ON (s.rating) s.rating, s.sid, s.sname, s.age
    FROM sailors s
    WHERE s.age = (
        SELECT MIN(s2.age)
        FROM sailors s2
        WHERE s2.rating = s.rating
    )
    ORDER BY s.rating DESC;
    """)

    pass

@pytest.mark.asyncio()
async def test_8_sailor_with_highest_res_per_boat(raw_db, test_db):
    """
    Select, for each boat, the sailor that made the highest number of reservations on that boat
    """
    raw_sql = text("""
    WITH reservation_counts AS (
        SELECT r.bid, r.sid, COUNT(*) AS reservation_count,
               RANK() OVER (PARTITION BY r.bid ORDER BY COUNT(*) DESC, r.sid ASC) AS rank
        FROM reserves r
        GROUP BY r.bid, r.sid
    )
    SELECT rc.bid, rc.sid, s.sname, rc.reservation_count
    FROM reservation_counts rc
    JOIN sailors s ON rc.sid = s.sid
    WHERE rc.rank = 1
    ORDER BY rc.bid;
    """)
    pass



