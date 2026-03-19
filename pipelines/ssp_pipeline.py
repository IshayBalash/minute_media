from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


def run(db: DBManager, run_date: str) -> None:
    """
    Insert external SSP revenue into the SSOT table.
    Covers site_type IN ('external', 'ext_player') — rows not tracked in events.
    Covers a 3-day lookback for late-arriving SSP reports.

    Args:
        db:         Active DBManager instance.
        run_date:   Date to process (YYYY-MM-DD).
    """
    db.execute(
        "DELETE FROM ssot WHERE event = 'ssp_external' AND date BETWEEN DATE(:run_date, '-3 days') AND :run_date",
        {"run_date": run_date},
    )
    db.execute_file("sql/pipelines/04_ssp_to_ssot.sql", {"run_date": run_date})
