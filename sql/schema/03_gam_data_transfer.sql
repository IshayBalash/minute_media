-- Source: Google Ad Manager Data Transfer
-- Contains actual (non-estimated) CPM for GAM network events
-- Used for CPM reconciliation of O&O and B2B GAM impressions
-- Arrives: within 8 hours (~4 PM for previous day's data)
-- NOTE (BigQuery): partition by DATE(timestamp)

CREATE TABLE IF NOT EXISTS gam_data_transfer (
    timestamp       TEXT    NOT NULL,  
    session_id      TEXT    NOT NULL,   
    ad_unit_id      TEXT,
    line_item_id    TEXT,
    country_name    TEXT,               
    impressions     INTEGER NOT NULL DEFAULT 0,
    cpm_usd         REAL    NOT NULL DEFAULT 0.0  
);
