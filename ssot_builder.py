"""
SSOT Builder — main pipeline orchestrator.

Runs hourly. Each run:
    1. Always: clear SSOT for run_date and re-insert from events (estimated revenue)
    2. Reconciliation steps run once per day at their scheduled hour,
       when their source data is known to be available.

Step schedule (all reconciliation steps cover the last 3 days):
    Every hour  — events → SSOT (estimated revenue)
    08:00 UTC   — SSP external revenue
    08:00 UTC   — Syndication revenue
    16:00 UTC   — GAM reconciliation (actual CPM, arrives ~4 PM)
    Next day 09:00 UTC — Demand partner reconciliation (O&O, arrives next day)

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
CURRENT_HOUR = 17  # fake flag for local testing


def run() -> None:
    db = DBManager()
    db.connect()
    ### only for testing/demo purposes: seed source tables with sample data
    seed(db)

    logger.info(f"\nStarting SSOT Builder for {RUN_DATE} at UTC hour {CURRENT_HOUR}.")
    count_records=db.query("SELECT count(1) as cnt_records FROM ssot""").iloc[0]['cnt_records'] 
    logger.info(f"SSOT record count: {count_records}")



    # --- Every hour: Step 1 clear run_date and rebuild from events ---
    logger.info("Clearing SSOT for run_date and inserting events.")
    db.execute("DELETE FROM ssot WHERE date = :run_date", {"run_date": RUN_DATE})
    count_records=db.query("SELECT count(1) as cnt_records FROM ssot""").iloc[0]['cnt_records'] 
    logger.info(f"SSOT record count: {count_records}")

    
    ## --- Every hour: Step 2: Insert records from events table to SSOT table ---
    logger.info("Inserting records from events table to SSOT table.")
    events_pipeline.run(db, RUN_DATE)
    count_records=db.query("SELECT count(1) as cnt_records FROM ssot""").iloc[0]['cnt_records'] 
    logger.info(f"SSOT record count: {count_records}")

    


    
    ## --- step 3: update GEMA reconciliation data (actual CPM) in SSOT table ---
    if (CURRENT_HOUR == 16) or (1==1):
        logger.info("Updating GAM reconciliation data (actual CPM) in SSOT table.")
        gam_reconciliation_pipeline.run(db, RUN_DATE)
        count_records=db.query("SELECT count(1) as cnt_records FROM ssot""").iloc[0]['cnt_records'] 
        logger.info(f"SSOT record count: {count_records}")

    

    ## --- step 4: update demand partner reconciliation data in SSOT table ---
    if (CURRENT_HOUR == 9) or (1==1):
        logger.info("Updating demand partner reconciliation data in SSOT table.")
        demand_partner_pipeline.run(db, RUN_DATE) 

    
    # --- 08:00 UTC: SSP external revenue (last 3 days) ---
    if CURRENT_HOUR == 8 or (1==1):
        logger.info("Inserting external SSP revenue into SSOT table.")
        ssp_pipeline.run(db, RUN_DATE)

    # --- 08:00 UTC: Syndication revenue (last 3 days) ---
    if CURRENT_HOUR == 8 or (1==1):
        logger.info("Inserting syndication revenue into SSOT table.")
        syndication_pipeline.run(db, RUN_DATE)
        count_records=db.query("SELECT count(1) as cnt_records FROM ssot""").iloc[0]['cnt_records'] 
        logger.info(f"SSOT record count: {count_records}")



    




    logger.info(f"Done for {RUN_DATE}.")
    db.disconnect()


run()



def test_run():
    db = DBManager()
    db.connect()
    db.execute("drop table if exists ssot")
    # query="""
    #     SELECT
    #      organization_id,
    #      count(1) as cnt_records
    #     from ssot
    #     group by organization_id
    #     """
    # result = db.query(query)
    # print(result.head(10))
    db.disconnect()



#test_run()