from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


def run(db: DBManager, run_date: str) -> None:
    """
    Insert syndication revenue into the SSOT table.
    Covers a 3-day lookback for late-arriving syndication reports.

    Args:
        db:         Active DBManager instance.
        run_date:   Date to process (YYYY-MM-DD).
    """
    db.execute(
        "DELETE FROM ssot WHERE event = 'syndication' AND date BETWEEN DATE(:run_date, '-3 days') AND :run_date",
        {"run_date": run_date},
    )
    db.execute_file("sql/pipelines/05_syndication_to_ssot.sql", {"run_date": run_date})
