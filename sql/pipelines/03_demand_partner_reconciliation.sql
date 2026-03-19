-- Pipeline: Demand partner reconciliation → SSOT
-- Updates estimated Prebid revenue with actual revenue from demand_partner_reports.
--
-- Scope:
--   - Non-GAM, non-MinuteSSP networks only (already reconciled by other pipelines)
--   - O&O properties only — enforced naturally by the JOIN to demand_partner_reports
--
-- Logic (CTEs):
--   1. demand_data     — last 3 days from demand_partner_reports
--   2. events_partners — re-aggregate events to get paying_entity at SSOT grain
--   3. ssot_totals     — total impressions per date + org + country + partner (denominator)
--   4. reconciled      — proportional revenue per SSOT row
--   5. UPDATE ssot
--
-- Revenue distribution:
--   Each SSOT row gets: partner_revenue × (row_event_count / total_event_count_for_property+country+partner+date)
--
-- Parameters:
--   :run_date  — date to process (YYYY-MM-DD). Covers run_date − 3 days to run_date.
--
-- NOTE (BigQuery): Replace DATE(..., '-3 days') with DATE_SUB(:run_date, INTERVAL 3 DAY)
--                  Replace this UPDATE with a MERGE statement

WITH demand_data AS (
    SELECT
        report_date,
        partner_name,
        property_code,
        geo,
        revenue_usd
    FROM demand_partner_reports
    WHERE report_date BETWEEN DATE(:run_date, '-3 days') AND :run_date
),

-- Re-aggregate events at SSOT grain to recover paying_entity
events_partners AS (
    SELECT
        DATE(timestamp)                                                     AS date,
        CAST(STRFTIME('%H', timestamp) AS INTEGER)                          AS rounded_hour,
        event,
        organization_id,
        adunit,
        media_type,
        network,
        CASE
            WHEN INSTR(url, '/') > 0 THEN SUBSTR(url, 1, INSTR(url, '/') - 1)
            ELSE url
        END                                                                 AS domain,
        line_item,
        ad_deal_type,
        demand_owner,
        country,
        -- paying_entity should be consistent within an SSOT group; MAX handles edge cases
        MAX(paying_entity)                                                  AS paying_entity
    FROM events
    WHERE DATE(timestamp) BETWEEN DATE(:run_date, '-3 days') AND :run_date
      AND network NOT IN ('GAM', 'MinuteSSP')
      AND event = 'served'
    GROUP BY
        DATE(timestamp),
        CAST(STRFTIME('%H', timestamp) AS INTEGER),
        event,
        organization_id,
        adunit,
        media_type,
        network,
        CASE
            WHEN INSTR(url, '/') > 0 THEN SUBSTR(url, 1, INSTR(url, '/') - 1)
            ELSE url
        END,
        line_item,
        ad_deal_type,
        demand_owner,
        country
),

-- Total impressions per date + org + country + paying_entity (denominator for proportional split)
ssot_totals AS (
    SELECT
        s.date,
        s.organization_id,
        s.country,
        ep.paying_entity,
        SUM(s.event_count) AS total_impressions
    FROM ssot s
    JOIN events_partners ep
      ON s.date            = ep.date
     AND s.rounded_hour    = ep.rounded_hour
     AND s.event           = ep.event
     AND s.organization_id = ep.organization_id
     AND COALESCE(s.adunit, '')       = COALESCE(ep.adunit, '')
     AND COALESCE(s.network, '')      = COALESCE(ep.network, '')
     AND COALESCE(s.domain, '')       = COALESCE(ep.domain, '')
     AND COALESCE(s.line_item, '')    = COALESCE(ep.line_item, '')
     AND COALESCE(s.ad_deal_type, '') = COALESCE(ep.ad_deal_type, '')
     AND COALESCE(s.demand_owner, '') = COALESCE(ep.demand_owner, '')
     AND COALESCE(s.country, '')      = COALESCE(ep.country, '')
    GROUP BY s.date, s.organization_id, s.country, ep.paying_entity
),

reconciled AS (
    SELECT
        s.date,
        s.rounded_hour,
        s.event,
        s.organization_id,
        s.adunit,
        s.media_type,
        s.network,
        s.domain,
        s.line_item,
        s.advertiser,
        s.ad_deal_type,
        s.demand_owner,
        s.country,
        -- Distribute partner revenue proportionally by impression share
        d.revenue_usd * (CAST(s.event_count AS REAL) / st.total_impressions) AS revenue
    FROM ssot s
    JOIN events_partners ep
      ON s.date            = ep.date
     AND s.rounded_hour    = ep.rounded_hour
     AND s.event           = ep.event
     AND s.organization_id = ep.organization_id
     AND COALESCE(s.adunit, '')       = COALESCE(ep.adunit, '')
     AND COALESCE(s.network, '')      = COALESCE(ep.network, '')
     AND COALESCE(s.domain, '')       = COALESCE(ep.domain, '')
     AND COALESCE(s.line_item, '')    = COALESCE(ep.line_item, '')
     AND COALESCE(s.ad_deal_type, '') = COALESCE(ep.ad_deal_type, '')
     AND COALESCE(s.demand_owner, '') = COALESCE(ep.demand_owner, '')
     AND COALESCE(s.country, '')      = COALESCE(ep.country, '')
    JOIN demand_data d
      ON s.date            = d.report_date
     AND s.organization_id = d.property_code
     AND LOWER(s.country)  = LOWER(d.geo)
     AND ep.paying_entity  = d.partner_name
    JOIN ssot_totals st
      ON s.date            = st.date
     AND s.organization_id = st.organization_id
     AND s.country         = st.country
     AND ep.paying_entity  = st.paying_entity
)

UPDATE ssot
SET revenue = reconciled.revenue
FROM reconciled
WHERE ssot.date            = reconciled.date
  AND ssot.rounded_hour    = reconciled.rounded_hour
  AND ssot.event           = reconciled.event
  AND ssot.organization_id = reconciled.organization_id
  AND COALESCE(ssot.adunit, '')        = COALESCE(reconciled.adunit, '')
  AND COALESCE(ssot.media_type, '')    = COALESCE(reconciled.media_type, '')
  AND COALESCE(ssot.network, '')       = COALESCE(reconciled.network, '')
  AND COALESCE(ssot.domain, '')        = COALESCE(reconciled.domain, '')
  AND COALESCE(ssot.line_item, '')     = COALESCE(reconciled.line_item, '')
  AND COALESCE(ssot.advertiser, '')    = COALESCE(reconciled.advertiser, '')
  AND COALESCE(ssot.ad_deal_type, '')  = COALESCE(reconciled.ad_deal_type, '')
  AND COALESCE(ssot.demand_owner, '')  = COALESCE(reconciled.demand_owner, '')
  AND COALESCE(ssot.country, '')       = COALESCE(reconciled.country, '');
