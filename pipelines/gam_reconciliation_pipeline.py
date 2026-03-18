from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


def run(db: DBManager, run_date: str) -> None:
    """
    Reconcile GAM revenue in the SSOT: update estimated CPM with actual
    CPM from gam_data_transfer. Covers a 3-day lookback for late arrivals.

    Args:
        db:         Active DBManager instance.
        run_date:   Date to process (YYYY-MM-DD).
    """
    db.execute_file("sql/pipelines/02_gam_reconciliation.sql", {"run_date": run_date})
