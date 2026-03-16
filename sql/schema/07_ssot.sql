-- Revenue Single Source of Truth (SSOT)
-- Consolidated, reconciled revenue across all streams
-- Full history retained (unlike events table which only keeps 60 days)
-- NOTE (BigQuery): partition by date, cluster by organization_id, event

CREATE TABLE IF NOT EXISTS ssot (
    date            TEXT    NOT NULL,   -- UTC date (YYYY-MM-DD)
    rounded_hour    INTEGER NOT NULL,   -- 0–23
    event           TEXT    NOT NULL,   -- served | pageView | videoEmbed | syndication | ssp_external
    organization_id TEXT    NOT NULL,
    adunit          TEXT,
    media_type      TEXT,
    network         TEXT,
    domain          TEXT,               -- Extracted from URL
    line_item       TEXT,
    advertiser      TEXT,               -- From line_items_mapping
    ad_deal_type    TEXT,
    demand_owner    TEXT,
    country         TEXT,
    revenue         REAL    NOT NULL DEFAULT 0.0,   -- Adjusted/reconciled revenue in USD
    event_count     INTEGER NOT NULL DEFAULT 0
);
