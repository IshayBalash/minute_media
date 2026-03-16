-- Source: daily reports from demand partners (Magnite, Triplelift, Index Exchange, etc.)
-- Used for CPM reconciliation of O&O properties only (not B2B publishers)
-- Does NOT contain GAM data
-- Arrives: 9 AM – next day (varies by partner)
-- NOTE (BigQuery): partition by report_date

CREATE TABLE IF NOT EXISTS demand_partner_reports (
    report_date     TEXT    NOT NULL,   -- UTC date (YYYY-MM-DD)
    partner_name    TEXT    NOT NULL,
    property_code   TEXT    NOT NULL,   -- Maps to organization_id for O&O properties
    ad_unit         TEXT,               -- Can be NULL (partner may not report at ad unit level)
    geo             TEXT,               -- Geography code (e.g. us, gb, fr)
    impressions     INTEGER NOT NULL DEFAULT 0,
    revenue_usd     REAL    NOT NULL DEFAULT 0.0
);
