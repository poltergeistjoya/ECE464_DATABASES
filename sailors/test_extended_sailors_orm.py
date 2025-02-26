import pytest
from sqlalchemy import func, create_engine, text, select, not_, exists
from sqlalchemy.orm import sessionmaker
from extended_orm_part3.extended_create_sailors_orm import (
    engine as orm_engine, 
    reserve_boat, 
    delete_reservation
    )
from datetime import date

@pytest.fixture(scope="function")
def test_db():
    """Connect to the ORM-based test database."""
    session = sessionmaker(bind=orm_engine)()
    yield session
    session.close()


def test_no_double_booking_same_boat_same_day(test_db):
    """
    Ensure that two different sailors cannot reserve the same boat on the same day 
    """
    sid_22, sid_64 = 22,64
    bid = 101
    reservation_date = date(2025, 3, 15)

    #first sailor 22 reserves 101 on 3/15/2025
    assert reserve_boat(test_db, sid_22, bid, reservation_date) is True

    #second sailor attempting to reserve should fail 
    assert reserve_boat(test_db, sid_64, bid, reservation_date) is False

    #cleanup
    assert delete_reservation(test_db, sid_22, bid, reservation_date) is True
