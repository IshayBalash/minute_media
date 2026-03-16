-- Source: daily aggregated report from our SSP infrastructure
-- Covers ALL SSP activity including external players/sites not in events table
-- Arrives: 8 AM daily
-- NOTE (BigQuery): partition by report_date

CREATE TABLE IF NOT EXISTS ssp_report (
    report_date     TEXT    NOT NULL,   -- UTC date (YYYY-MM-DD)
    publisher_id    TEXT    NOT NULL,
    placement_type  TEXT    NOT NULL,   -- display | video
    site_type       TEXT    NOT NULL,   -- own_site | external | own_player | ext_player
    country_code    TEXT,               -- ISO 3166-1 alpha-3 (e.g. USA, GBR)
    impressions     INTEGER NOT NULL DEFAULT 0,
    revenue_usd     REAL    NOT NULL DEFAULT 0.0
);
