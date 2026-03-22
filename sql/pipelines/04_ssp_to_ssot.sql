-- Pipeline: SSP external revenue → SSOT
-- Inserts external SSP revenue that is NOT tracked in the events table.
--
-- Scope:
--   - site_type IN ('external', 'ext_player') only
--   - own_site / own_player are already in events with better granularity → excluded
--
-- Field mapping:
--   organization_id = publisher_id
--   media_type      = placement_type  (display | video)
--   network         = 'MinuteSSP'     (our SSP won these impressions)
--   rounded_hour    = NULL            (daily data, no hourly breakdown)
--   revenue         = revenue_usd     (real revenue, not estimated)
--   event_count     = impressions
--   all other fields (adunit, domain, line_item, etc.) = NULL
--
-- Parameters:
--   :run_date  — date to process (YYYY-MM-DD). Covers run_date − 3 days to run_date.
--

WITH ssp_external AS (
    SELECT
        report_date,
        publisher_id,
        placement_type,
        country_code,
        impressions,
        revenue_usd
    FROM ssp_report
    WHERE site_type IN ('external', 'ext_player')
      AND report_date BETWEEN DATE(:run_date, '-3 days') AND :run_date
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
    report_date         AS date,
    NULL                AS rounded_hour,
    'ssp_external'      AS event,
    publisher_id        AS organization_id,
    NULL                AS adunit,
    placement_type      AS media_type,
    'MinuteSSP'         AS network,
    NULL                AS domain,
    NULL                AS line_item,
    NULL                AS advertiser,
    NULL                AS ad_deal_type,
    NULL                AS demand_owner,
    country_code        AS country,
    revenue_usd         AS revenue,
    impressions         AS event_count
FROM ssp_external;
