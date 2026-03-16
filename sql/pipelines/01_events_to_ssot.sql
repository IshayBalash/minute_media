-- Pipeline: events → SSOT
-- Aggregates all events for a given date and inserts them into the SSOT table.
-- Revenue is estimated CPM / 1000 per impression (except MinuteSSP which is real CPM).
-- GAM and demand partner reconciliation happens in subsequent pipeline steps.
--
-- Parameters:
--   :run_date  — date to process (YYYY-MM-DD)
--
-- NOTE (BigQuery): Replace DATE(timestamp) with DATE(TIMESTAMP_TRUNC(timestamp, DAY))
--                  Replace STRFTIME('%H', timestamp) with EXTRACT(HOUR FROM timestamp)
--                  Replace SUBSTR/INSTR domain logic with REGEXP_EXTRACT or NET.HOST(url)

INSERT INTO ssot (
    date,
    rounded_hour,
    event,
    organization_id,
    adunit,
    media_type,
    network,
    domain,
    line_item,
    advertiser,
    ad_deal_type,
    demand_owner,
    country,
    revenue,
    event_count
)
SELECT
    DATE(e.timestamp)                                                   AS date,
    CAST(STRFTIME('%H', e.timestamp) AS INTEGER)                        AS rounded_hour,
    e.event,
    e.organization_id,
    e.adunit,
    e.media_type,
    e.network,

    -- Extract domain from URL (strip everything from first '/' onward)
    CASE
        WHEN INSTR(e.url, '/') > 0 THEN SUBSTR(e.url, 1, INSTR(e.url, '/') - 1)
        ELSE e.url
    END                                                                 AS domain,

    e.line_item,
    lim.advertiser,
    e.ad_deal_type,
    e.demand_owner,
    e.country,

    -- Revenue = sum of (CPM / 1000) per impression.
    -- For MinuteSSP this is already real revenue.
    -- For all other networks this is estimated and will be reconciled in later steps.
    SUM(e.cpm) / 1000.0                                                AS revenue,

    COUNT(*)                                                            AS event_count

FROM events e
LEFT JOIN line_items_mapping lim ON e.line_item = lim.line_item_id

WHERE DATE(e.timestamp) = :run_date

GROUP BY
    DATE(e.timestamp),
    CAST(STRFTIME('%H', e.timestamp) AS INTEGER),
    e.event,
    e.organization_id,
    e.adunit,
    e.media_type,
    e.network,
    -- Group by the same domain expression
    CASE
        WHEN INSTR(e.url, '/') > 0 THEN SUBSTR(e.url, 1, INSTR(e.url, '/') - 1)
        ELSE e.url
    END,
    e.line_item,
    lim.advertiser,
    e.ad_deal_type,
    e.demand_owner,
    e.country
