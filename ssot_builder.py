"""
SSOT Builder — main pipeline orchestrator.

Runs hourly. Each run:
    1. Always: delete last 2 hours from SSOT and re-insert from events (estimated revenue).
    2. Reconciliation/insert steps run at specific hours (with a 2-hour window to
       match the events refresh, so reconciled data is re-applied after re-insert).

Step schedule:
    Every hour      — events → SSOT (last 2 hours, estimated revenue)
    08:00–09:00 UTC — SSP external revenue (3-day lookback)
    08:00–09:00 UTC — Syndication revenue (3-day lookback)
    16:00–17:00 UTC — GAM reconciliation (3-day lookback, actual CPM)
    09:00–10:00 UTC — Demand partner reconciliation (3-day lookback, O&O)

How to run:
    python3 ssot_builder.py
"""

from datetime import date
from db.manager import DBManager
from db.seed_data import seed
from pipelines import events_pipeline, gam_reconciliation_pipeline, demand_partner_pipeline, ssp_pipeline, syndication_pipeline
from utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

RUN_DATE = "2024-01-15"

# TODO: replace with datetime.now(timezone.utc).hour in production
CURRENT_HOUR = 8


def run() -> None:
    db = DBManager()
    db.connect()
    ### only for testing/demo purposes: seed source tables with sample data
    seed(db)

    logger.info(f"\nStarting SSOT Builder for {RUN_DATE} at UTC hour {CURRENT_HOUR}.")
    count_records=db.query("SELECT count(1) as cnt_records FROM ssot""").iloc[0]['cnt_records'] 
    logger.info(f"SSOT record count: {count_records}")



    # --- Every hour: delete last 2 hours and re-insert from events ---
    logger.info(f"Clearing SSOT for hours {max(0, CURRENT_HOUR - 1)}–{CURRENT_HOUR} and re-inserting from events.")
    db.execute(
        "DELETE FROM ssot WHERE date = :run_date AND rounded_hour BETWEEN (:current_hour - 1) AND :current_hour",
        {"run_date": RUN_DATE, "current_hour": CURRENT_HOUR},
    )
    events_pipeline.run(db, RUN_DATE, CURRENT_HOUR)

    


    # --- 08:00–09:00 UTC: SSP external revenue (3-day lookback) ---
    if CURRENT_HOUR in (8, 9) or 1==1:
        logger.info("Inserting external SSP revenue into SSOT table.")
        ssp_pipeline.run(db, RUN_DATE)

    # --- 08:00–09:00 UTC: Syndication revenue (3-day lookback) ---
    if CURRENT_HOUR in (8, 9)or 1==1:
        logger.info("Inserting syndication revenue into SSOT table.")
        syndication_pipeline.run(db, RUN_DATE)

    # --- 16:00–17:00 UTC: GAM reconciliation (actual CPM, 3-day lookback) ---
    if CURRENT_HOUR in (16, 17)or 1==1:
        logger.info("Updating GAM reconciliation data (actual CPM) in SSOT table.")
        gam_reconciliation_pipeline.run(db, RUN_DATE)

    # --- 09:00–10:00 UTC: Demand partner reconciliation (O&O, 3-day lookback) ---
    if CURRENT_HOUR in (9, 10)or 1==1:
        logger.info("Updating demand partner reconciliation data in SSOT table.")
        demand_partner_pipeline.run(db, RUN_DATE)


    logger.info(f"Done for {RUN_DATE}.")
    db.disconnect()


run()



# def test_run():
#     db = DBManager()
#     db.connect()
#     db.execute("drop table if exists ssot")
#     query="""
#         delete from ssot
#         """
#     db.query(query)
#     db.disconnect()



# test_run()