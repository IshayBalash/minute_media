from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


def run(db: DBManager, run_date: str, current_hour: int) -> None:
    """
    Aggregate events for the last 2 hours of run_date and insert them into the SSOT table.

    Args:
        db:           Active DBManager instance.
        run_date:     Date to process (YYYY-MM-DD).
        current_hour: Current UTC hour (0–23).
    """
    db.execute_file("sql/pipelines/01_events_to_ssot.sql", {"run_date": run_date, "current_hour": current_hour})
