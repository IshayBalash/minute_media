-- Source: first-party event-level raw data
-- Volume: billions of rows/day | Retention: last 60 days
-- NOTE (BigQuery): partition by DATE(timestamp), cluster by organization_id, event

CREATE TABLE IF NOT EXISTS events (
    timestamp       TEXT        NOT NULL,   -- UTC datetime (ISO 8601)
    event           TEXT        NOT NULL,   -- pageView | served | videoEmbed
    organization_id TEXT        NOT NULL,
    adunit          TEXT,
    media_type      TEXT,
    network         TEXT,
    cpm             REAL        NOT NULL DEFAULT 0.0,  -- Estimated (except MinuteSSP = real)
    sessionid       TEXT,
    url             TEXT,
    line_item       TEXT,
    ad_deal_type    TEXT,
    demand_owner    TEXT,
    paying_entity   TEXT,
    country         TEXT
);
