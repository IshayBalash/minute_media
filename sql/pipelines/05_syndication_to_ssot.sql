-- Pipeline: Syndication revenue → SSOT
-- Inserts content syndication revenue from third-party partners (Yahoo Sports, MSN, etc.)
--
-- Field mapping:
--   organization_id = content_property
--   advertiser      = partner_name  (the syndication partner)
--   revenue         = total_revenue
--   event_count     = article_count
--   rounded_hour    = NULL          (daily data, no hourly breakdown)
--   all other fields (adunit, network, domain, etc.) = NULL
--
-- Parameters:
--   :run_date  — date to process (YYYY-MM-DD). Covers run_date − 3 days to run_date.
--

WITH syndication_data AS (
    SELECT
        transaction_date,
        partner_name,
        content_property,
        article_count,
        total_revenue
    FROM syndication_revenue
    WHERE transaction_date BETWEEN DATE(:run_date, '-3 days') AND :run_date
)
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
    transaction_date    AS date,
    NULL                AS rounded_hour,
    'syndication'       AS event,
    content_property    AS organization_id,
    NULL                AS adunit,
    NULL                AS media_type,
    NULL                AS network,
    NULL                AS domain,
    NULL                AS line_item,
    partner_name        AS advertiser,
    NULL                AS ad_deal_type,
    NULL                AS demand_owner,
    NULL                AS country,
    total_revenue       AS revenue,
    article_count       AS event_count
FROM syndication_data;
