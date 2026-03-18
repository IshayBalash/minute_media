-- Pipeline: GAM reconciliation → SSOT
-- Updates estimated GAM revenue with actual CPM from gam_data_transfer.
--
-- Logic (CTEs):
--   1. gam_events     — GAM events from the last 3 days
--   2. gam_actuals    — GAM data transfer records from the last 3 days
--   3. reconciled     — LEFT JOIN + aggregate at SSOT grain, COALESCE to actual CPM
--   4. UPDATE ssot    — merge on all grain columns
--
-- Parameters:
--   :run_date  — date to process (YYYY-MM-DD). Reconciliation covers run_date − 3 days to run_date.
--

WITH gam_events AS (
    SELECT *
    FROM events
    WHERE network = 'GAM'
      AND DATE(timestamp) BETWEEN DATE(:run_date, '-3 days') AND :run_date
),

gam_actuals AS (
    SELECT session_id, line_item_id, cpm_usd
    FROM gam_data_transfer
    WHERE DATE(timestamp) BETWEEN DATE(:run_date, '-3 days') AND :run_date
),

reconciled AS (
    SELECT
        DATE(e.timestamp)                                                   AS date,
        CAST(STRFTIME('%H', e.timestamp) AS INTEGER)                        AS rounded_hour,
        e.event,
        e.organization_id,
        e.adunit,
        e.media_type,
        e.network,

        CASE
            WHEN INSTR(e.url, '/') > 0 THEN SUBSTR(e.url, 1, INSTR(e.url, '/') - 1)
            ELSE e.url
        END                                                                 AS domain,

        e.line_item,
        lim.advertiser,
        e.ad_deal_type,
        e.demand_owner,
        e.country,
        SUM(COALESCE(g.cpm_usd, e.cpm)) / 1000.0                          AS revenue

    FROM gam_events e
    LEFT JOIN gam_actuals g
           ON LOWER(e.sessionid) = LOWER(g.session_id)
          AND e.line_item = g.line_item_id
    LEFT JOIN line_items_mapping lim
           ON e.line_item = lim.line_item_id

    GROUP BY
        DATE(e.timestamp),
        CAST(STRFTIME('%H', e.timestamp) AS INTEGER),
        e.event,
        e.organization_id,
        e.adunit,
        e.media_type,
        e.network,
        CASE
            WHEN INSTR(e.url, '/') > 0 THEN SUBSTR(e.url, 1, INSTR(e.url, '/') - 1)
            ELSE e.url
        END,
        e.line_item,
        lim.advertiser,
        e.ad_deal_type,
        e.demand_owner,
        e.country
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
