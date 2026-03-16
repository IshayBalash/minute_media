"""
SSOT Builder — main pipeline orchestrator.

Execution order:
    1. Clear SSOT for the given date (idempotent — safe to re-run)
    2. Insert events
    3. Insert SSP external revenue          (coming soon)
    4. Insert syndication revenue           (coming soon)
    5. Update revenue from GAM              (coming soon)
    6. Update revenue from demand partners  (coming soon)

Usage:
    python3 ssot_builder.py
"""

from db.manager import DBManager
from db.seed_data import seed
from pipelines import events_pipeline
from utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

RUN_DATE = "2024-01-15"


def run() -> None:

    logger.info(f"-------------------Starting SSOT Builder for {RUN_DATE}. ---------------------------------\n")

    logger.info(f"------------------- Connecting to DB. ---------------------------------\n")
    db = DBManager()
    db.connect()

    ### only for testing/demo purposes: seed source tables with sample data
    logger.info(f"------------------- Seeding source tables. ---------------------------------\n")
    # Seed source tables (truncates and reloads sample data)
    seed(db)

    # Clear SSOT for this date before running all pipelines
    logger.info(f"Clearing SSOT for {RUN_DATE} before processing.")
    db.execute_sql_string("DELETE FROM ssot WHERE date = :run_date", {"run_date": RUN_DATE})

    logger.info(f"------------------- Running Events Pipeline ---------------------------------\n")
    # Step 1: events → SSOT (estimated revenue)
    events_pipeline.run(db, RUN_DATE)

    # Step 2: SSP external revenue → SSOT         (TODO)
    # Step 3: Syndication revenue → SSOT           (TODO)
    # Step 4: Update GAM revenue (reconciliation)  (TODO)
    # Step 5: Update demand partner revenue (O&O)  (TODO)

    logger.info(f"Done for {RUN_DATE}.")
    db.disconnect()


run()
