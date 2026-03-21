# Revenue SSOT — Solution

## Overview

This project builds a **Revenue Single Source of Truth (SSOT)** for Minute Media by consolidating all revenue streams into a single, granular, daily-refreshed table. The pipeline runs hourly, pulling from five source tables and applying a layered reconciliation strategy to produce accurate, adjusted revenue figures.

The implementation uses **SQLite** locally (for development/demo) and is designed to run on **BigQuery** in production. All SQL files include BigQuery migration notes.

---

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python3 ssot_builder.py
```

> `ssot_builder.py` seeds the database with sample data, then runs all pipeline steps.
> Set `RUN_DATE` and `CURRENT_HOUR` at the top of the file to simulate different run times.

---

## Project Structure

```
├── ssot_builder.py              # Main orchestrator — run this
├── config/
│   └── constants.py             # DB path
├── db/
│   ├── manager.py               # DBManager — all DB interactions
│   └── seed_data.py             # Sample data loader (for local testing)
├── pipelines/
│   ├── events_pipeline.py       # Step 1: events → SSOT
│   ├── gam_reconciliation_pipeline.py   # Step 2: GAM actual CPM
│   ├── ssp_pipeline.py          # Step 3: SSP external revenue
│   ├── syndication_pipeline.py  # Step 4: Syndication revenue
│   └── demand_partner_pipeline.py       # Step 5: Prebid O&O reconciliation
├── sql/
│   ├── schema/                  # CREATE TABLE definitions (01–07)
│   └── pipelines/               # Pipeline SQL queries (01–05)
└── utils/
    └── logger.py                # Logging setup
```

---

## Pipeline Architecture

```
Every hour (all sources use 3-day lookback for reconciliation steps)
│
├── [Every hour]
│   ├── DELETE ssot WHERE date = run_date
│   └── 01_events_to_ssot.sql ──────────────── INSERT from events (estimated CPM)
│
├── [08:00 UTC]
│   ├── 04_ssp_to_ssot.sql ─────────────────── INSERT SSP external rows
│   └── 05_syndication_to_ssot.sql ─────────── INSERT syndication rows
│
├── [16:00 UTC]
│   └── 02_gam_reconciliation.sql ──────────── UPDATE GAM rows → actual CPM
│
└── [Next day 09:00 UTC]
    └── 03_demand_partner_reconciliation.sql ── UPDATE Prebid O&O rows → actual revenue
```

### Why this schedule?

| Source | Arrives | Step runs |
|---|---|---|
| `events` | Streaming (continuous) | Every hour |
| `ssp_report` | 8 AM UTC | 08:00 UTC |
| `syndication_revenue` | 8 AM UTC | 08:00 UTC |
| `gam_data_transfer` | ~4 PM UTC (within 8h) | 16:00 UTC |
| `demand_partner_reports` | 9 AM UTC next day | Next day 09:00 UTC |

**Idempotency:** Each run deletes and rebuilds only the `run_date` slice from events. Reconciliation steps operate on a **3-day lookback window** to catch late-arriving data without re-running from scratch.

---

## Pipeline Steps

### Step 1 — Events → SSOT (`01_events_to_ssot.sql`)

**Runs:** Every hour
**Type:** DELETE + INSERT

Aggregates all raw events for `run_date` and inserts them into the SSOT at hourly grain (date + hour + all dimension columns).

**Logic:**
- Groups by all SSOT dimension columns
- Revenue = `SUM(cpm) / 1000.0` per group (estimated CPM per impression)
- MinuteSSP CPM is real — no reconciliation needed for those rows
- Joins `line_items_mapping` to populate the `advertiser` field
- Domain extracted from URL by stripping everything from the first `/` onward

**Assumptions:**
- All event types (`served`, `pageView`, `videoEmbed`) are inserted — not just revenue-generating events — to preserve full event counts in the SSOT
- CPM from events is treated as estimated for all networks except MinuteSSP
- A session can have at most one GAM match per line item (session_id + line_item_id is effectively unique in `gam_data_transfer`)

---

### Step 2 — GAM Reconciliation (`02_gam_reconciliation.sql`)

**Runs:** 16:00 UTC daily
**Type:** UPDATE (3-day lookback)

Replaces estimated CPM with actual CPM from `gam_data_transfer` for all GAM network rows.

**Logic (CTEs):**
1. `gam_events` — filters events to `network = 'GAM'`, last 3 days
2. `gam_actuals` — filters `gam_data_transfer` to last 3 days
3. `reconciled` — LEFT JOINs events to GAM actuals on `LOWER(sessionid) = LOWER(session_id)` AND `line_item = line_item_id`. Uses `COALESCE(g.cpm_usd, e.cpm)` — if no GAM match, falls back to estimated CPM
4. UPDATE SSOT on all grain columns

**Assumptions:**
- `session_id` in `gam_data_transfer` is always lowercase; events `sessionid` is mixed case → `LOWER()` on both sides
- GAM data covers all GAM impressions for the period; unmatched rows (late/missing) fall back to estimated CPM
- 3-day lookback handles late-arriving GAM data transfer files

---

### Step 3 — SSP External Revenue (`04_ssp_to_ssot.sql`)

**Runs:** 08:00 UTC daily
**Type:** DELETE + INSERT (3-day lookback)

Inserts SSP revenue for publisher/player inventory that is **not tracked in the events table**.

**Logic:**
- Filters `ssp_report` to `site_type IN ('external', 'ext_player')` only
- `own_site` and `own_player` rows are excluded — those are already in `events` with better hourly granularity
- DELETE existing `ssp_external` rows for the 3-day window, then re-INSERT

**Field mapping:**

| SSOT field | Source | Notes |
|---|---|---|
| `organization_id` | `publisher_id` | |
| `media_type` | `placement_type` | display / video |
| `network` | `'MinuteSSP'` | Our SSP won these impressions |
| `revenue` | `revenue_usd` | Already total daily revenue, not per-impression |
| `event_count` | `impressions` | |
| `rounded_hour` | `NULL` | Daily data — no hourly breakdown available |
| `adunit`, `domain`, etc. | `NULL` | Not available in SSP report |

**Assumptions:**
- SSP report revenue is already the true total (not estimated) — no further reconciliation needed
- `own_site` / `own_player` rows in `ssp_report` have identical revenue to the corresponding `events` rows; events is treated as authoritative for those
- Country code format in `ssp_report` (ISO alpha-3, e.g. `USA`) is preserved as-is

---

### Step 4 — Syndication Revenue (`05_syndication_to_ssot.sql`)

**Runs:** 08:00 UTC daily
**Type:** DELETE + INSERT (3-day lookback)

Inserts content syndication revenue from third-party partners (Yahoo Sports, MSN, etc.). This stream has no representation in the events table.

**Field mapping:**

| SSOT field | Source | Notes |
|---|---|---|
| `organization_id` | `content_property` | |
| `advertiser` | `partner_name` | The syndication partner paying for the content |
| `revenue` | `total_revenue` | Flat fee + revenue share combined |
| `event_count` | `article_count` | |
| `rounded_hour` | `NULL` | Daily data |
| `country`, `network`, etc. | `NULL` | Not available in syndication reports |

**Assumptions:**
- Syndication revenue is flat/fixed per period — no CPM reconciliation needed
- `total_revenue = flat_fee + revenue_share` is the authoritative figure
- No country-level breakdown is available from syndication partners

---

### Step 5 — Demand Partner Reconciliation (`03_demand_partner_reconciliation.sql`)

**Runs:** Next day 09:00 UTC
**Type:** UPDATE (3-day lookback)

Updates estimated Prebid revenue for O&O properties using actual revenue from demand partner reports (Magnite, Triplelift, IndexExchange, etc.).

**Logic (CTEs):**
1. `demand_data` — last 3 days from `demand_partner_reports`
2. `events_partners` — re-aggregates events at SSOT grain to recover `paying_entity` (the demand partner). Filters to non-GAM, non-MinuteSSP, `served` events only
3. `ssot_totals` — calculates total impressions per `date + org + country + paying_entity` (the denominator for proportional distribution)
4. `reconciled` — joins SSOT → events_partners → demand_data → ssot_totals and distributes partner revenue proportionally: `revenue = partner_revenue × (row_event_count / total_impressions)`
5. UPDATE SSOT on all grain columns

**Assumptions:**
- O&O scope is enforced by the JOIN to `demand_partner_reports` — publishers not in that table are naturally excluded
- `paying_entity` in events maps directly to `partner_name` in demand partner reports
- Within a given SSOT grain group, `paying_entity` is consistent; `MAX(paying_entity)` handles any edge-case variance
- Revenue is distributed proportionally by impression count — this is the standard approach when partner reports don't provide sub-property breakdown
- `demand_partner_reports.geo` uses lowercase country codes (e.g. `us`, `gb`) → matched with `LOWER(ssot.country)`
- GAM and MinuteSSP rows are excluded — they are reconciled by dedicated pipelines

---

## SSOT Table Schema

| Field | Type | Notes |
|---|---|---|
| `date` | TEXT | UTC date (YYYY-MM-DD) |
| `rounded_hour` | INTEGER | 0–23, NULL for daily sources (SSP, syndication) |
| `event` | TEXT | `served`, `pageView`, `videoEmbed`, `ssp_external`, `syndication` |
| `organization_id` | TEXT | Property / publisher identifier |
| `adunit` | TEXT | Ad unit identifier |
| `media_type` | TEXT | `banner`, `video` |
| `network` | TEXT | `GAM`, `Prebid`, `MinuteSSP`, etc. |
| `domain` | TEXT | Extracted from event URL |
| `line_item` | TEXT | Ad line item ID |
| `advertiser` | TEXT | From `line_items_mapping` or syndication partner |
| `ad_deal_type` | TEXT | `rtb`, `direct` |
| `demand_owner` | TEXT | Entity owning the demand relationship |
| `country` | TEXT | User country |
| `revenue` | REAL | Adjusted/reconciled revenue in USD |
| `event_count` | INTEGER | Count of events / impressions / articles |

---

## General Assumptions

- All timestamps and dates are **UTC**
- The `events` table retains only the last 60 days; the SSOT retains full history
- Re-running the full pipeline for a date is safe (idempotent) — `run_date` is cleared and rebuilt each run
- Reconciliation steps use a **3-day lookback** to handle late-arriving source data without a full historical reprocess
