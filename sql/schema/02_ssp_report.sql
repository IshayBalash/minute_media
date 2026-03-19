
CREATE TABLE IF NOT EXISTS ssp_report (
    report_date     TEXT    NOT NULL,  
    placement_type  TEXT    NOT NULL,   
    country_code    TEXT,              
    impressions     INTEGER NOT NULL DEFAULT 0,
    revenue_usd     REAL    NOT NULL DEFAULT 0.0
);
