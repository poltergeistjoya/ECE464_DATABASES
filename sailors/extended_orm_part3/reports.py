from sqlalchemy import (
    case, 
    literal
)
from datetime import date
from extended_create_sailors_orm import init_db, session, Boat, Reserve

def get_daily_inventory(test_db, report_date):
    """
    Generates a report of boat availability for a given date
    """

    inventory_report = (
        test_db.query(
            Boat.bid, 
            Boat.bname,
            case(
                (Reserve.bid.isnot(None), literal("Reserved")), 
                else_=literal("Available") 
            ).label("status")
        )
        .outerjoin(Reserve, (Boat.bid == Reserve.bid) & (Reserve.day == report_date))  # LEFT JOIN
        .order_by("status")
        .all()
    )
    
    # Print Report
    print(f"Inventory Report for {report_date}:")
    for row in inventory_report:
        print(row)

    return inventory_report


if __name__ == "__main__":
    init_db()

    report_date = date(1998, 11, 12)

    _ = get_daily_inventory(session, report_date)
