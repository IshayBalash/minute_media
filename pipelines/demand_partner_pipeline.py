from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


def run(db: DBManager, run_date: str) -> None:
    """
    Reconcile Prebid revenue for O&O properties using demand_partner_reports.
    Joins back to events to recover paying_entity, then distributes partner
    revenue proportionally by impression share across matching SSOT rows.
    Covers a 3-day lookback for late-arriving partner reports.

    Args:
        db:         Active DBManager instance.
        run_date:   Date to process (YYYY-MM-DD).
    """
    db.execute_file("sql/pipelines/03_demand_partner_reconciliation.sql", {"run_date": run_date})
