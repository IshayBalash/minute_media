from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


def run(db: DBManager, run_date: str) -> None:
    """
    Aggregate all events for run_date and insert them into the SSOT table.

    Args:
        db:         Active DBManager instance.
        run_date:   Date to process (YYYY-MM-DD).
    """
    db.run_sql_file("sql/pipelines/01_events_to_ssot.sql", {"run_date": run_date})

    count = db.query_from_sql_string(
        "SELECT COUNT(*) AS cnt FROM ssot WHERE date = :run_date",
        {"run_date": run_date},
    )
    logger.info(f"Inserted {count['cnt'][0]} rows into SSOT for {run_date}.")