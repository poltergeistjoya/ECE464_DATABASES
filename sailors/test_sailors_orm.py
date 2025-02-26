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

def test_3_sailors_reserve_only_red_boats(raw_db, test_db):
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

    raw_results = raw_db.execute(raw_sql).fetchall()


    sailor_has_1_reserve = select(Reserve).filter(Reserve.sid == Sailor.sid)

    sailor_has_non_red_reserve = select(Reserve.bid).join(Boat).filter(Reserve.sid == Sailor.sid, Boat.color != 'red')

    orm_results = (
        test_db.query(Sailor.sid, Sailor.sname)
        .filter(sailor_has_1_reserve.exists())
        .filter(~sailor_has_non_red_reserve.exists())
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


def test_4_boat_with_most_reservations(raw_db, test_db):
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

    raw_results = raw_db.execute(raw_sql).fetchall()

    boat_reservations = (select(Boat.bname, Reserve.bid, func.count(Reserve.sid).label("reservation_count"))
    .join(Boat, Reserve.bid ==  Boat.bid)
    .group_by(Reserve.bid, Boat.bname)
    .order_by(func.count(Reserve.sid))
    ).subquery()

    max_reservation = select(func.max(boat_reservations.c.reservation_count)).scalar_subquery()

    top_boat = (
        test_db.query(boat_reservations.c.bname, boat_reservations.c.bid, boat_reservations.c.reservation_count)
        .filter(boat_reservations.c.reservation_count == max_reservation)
        .all()
    )

    # Convert results to tuples for direct comparison
    raw_results = [(row[0].strip(), row[1], row[2]) for row in raw_results]
    orm_results = [(row[0], row[1], row[2]) for row in top_boat]

    # Print for debugging
    print("\nORM Results:")
    for row in orm_results:
        print(row)

    print("\nRaw SQL Results:")
    for row in raw_results:
        print(row)

    # Ensure both results match
    assert orm_results == raw_results, "ORM and raw SQL results do not match!"
    

def test_5_sailors_never_reserve_red_boat(raw_db, test_db):
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

    raw_results = raw_db.execute(raw_sql).fetchall()

    red_boats_reserved_by_sailor = select(Reserve.bid).join(Boat, Reserve.bid == Boat.bid).filter(Reserve.sid == Sailor.sid).filter(Boat.color == 'red')

    orm_results = test_db.query(Sailor.sid, Sailor.sname).filter(~(red_boats_reserved_by_sailor).exists()).all()

    raw_results = [(row[0], row[1].strip()) for row in raw_results] #strip empty space in sname 
    orm_results = [(row[0], row[1]) for row in orm_results]

    print("\nORM Results:")
    for row in orm_results:
        print(row)

    print("\nRaw SQL Results:")
    for row in raw_results:
        print(row)

    assert orm_results == raw_results, "ORM and raw SQL results do not match!"

def test_6_avg_age_sailors_rating_10(raw_db, test_db):
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

    boat_reservations = (select(Boat.bname, Reserve.bid, func.count(Reserve.sid).label("reservation_count"))
    .join(Boat, Reserve.bid ==  Boat.bid)
    .group_by(Reserve.bid, Boat.bname)
    .order_by(func.count(Reserve.sid))
    ).subquery()


    pass

def test_7_youngest_sailor_for_each_rating(raw_db, test_db):
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
    raw_results = raw_db.execute(raw_sql).fetchall()

    min_age_by_rating = (select(Sailor.rating, func.min(Sailor.age).label("min_age"))
               .group_by(Sailor.rating)
               .subquery()
    )

    orm_results = (
        test_db.query(Sailor.rating, Sailor.sid, Sailor.sname, Sailor.age)
        .join(min_age_by_rating,
              (Sailor.rating == min_age_by_rating.c.rating) & 
              (Sailor.age == min_age_by_rating.c.min_age)
        )
        .distinct(Sailor.rating)
        .order_by(Sailor.rating.desc())
        .all()
    )

    # Convert results to tuples for direct comparison
    raw_results = [(row[0], row[1], row[2].strip(), row[3]) for row in raw_results]
    orm_results = [(row[0], row[1], row[2], row[3]) for row in orm_results]

    # Print for debugging
    print("\nORM Results:")
    for row in orm_results:
        print(row)

    print("\nRaw SQL Results:")
    for row in raw_results:
        print(row)

    # Ensure both results match
    assert orm_results == raw_results, "ORM and raw SQL results do not match!"

def test_8_sailor_with_highest_res_per_boat(raw_db, test_db):
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

    raw_results = raw_db.execute(raw_sql).fetchall()

    reservation_counts_by_boat = (
        select(
            Reserve.bid, 
            Reserve.sid, 
            func.count().label("reservation_count"),
            func.rank().over(
                partition_by=Reserve.bid, order_by=[func.count().desc(), Reserve.sid.asc()]
            ).label("rank")
        )
        .group_by(Reserve.bid, Reserve.sid)
        .subquery()
    )

    orm_results = (
        test_db.query(
            reservation_counts_by_boat.c.bid, 
            reservation_counts_by_boat.c.sid, 
            Sailor.sname,
            reservation_counts_by_boat.c.reservation_count
        )
        .join(Sailor, reservation_counts_by_boat.c.sid == Sailor.sid)
        .filter(reservation_counts_by_boat.c.rank==1)
        .order_by(reservation_counts_by_boat.c.bid)
        .all()
    )

    # Convert results to tuples for direct comparison
    raw_results = [(row[0], row[1], row[2].strip(), row[3]) for row in raw_results]
    orm_results = [(row[0], row[1], row[2], row[3]) for row in orm_results]

    # Print for debugging
    print("\nORM Results:")
    for row in orm_results:
        print(row)

    print("\nRaw SQL Results:")
    for row in raw_results:
        print(row)

    # Ensure both results match
    assert orm_results == raw_results, "ORM and raw SQL results do not match!"


