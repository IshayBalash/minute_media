# Revenue SSOT — Solution

## Overview

This project builds a **Revenue Single Source of Truth (SSOT)** by consolidating all revenue streams into a single, granular, daily-refreshed table. The pipeline runs hourly, pulling from five source tables 
to produce accurate, adjusted revenue figures.

The implementation uses **SQLite** locally (for development/demo) and can be easy ajusted to other SQL engines.

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
│   ├── schema/                  # CREATE TABLE definitions 
│   └── pipelines/               # Pipeline SQL queries 
└── utils/
    └── logger.py                # Logging setup
```

---

## Pipeline Architecture

```
├── [Every hour]
│   ├── DELETE ssot WHERE date = run_date AND rounded_hour IN (current, current-1)
│   └── 01_events_to_ssot.sql ──────────────────── INSERT last 2 hours from events (estimated CPM)
│
├── [08:00–09:00 UTC]
│   ├── DELETE ssp_external rows (3-day window)
│   ├── 04_ssp_to_ssot.sql ─────────────────────── INSERT SSP external rows
│   ├── DELETE syndication rows (3-day window)
│   └── 05_syndication_to_ssot.sql ─────────────── INSERT syndication rows
│
├── [16:00–17:00 UTC]
│   └── 02_gam_reconciliation.sql ──────────────── UPDATE existing GAM rows → actual CPM
│
└── [09:00–10:00 UTC]
    └── 03_demand_partner_reconciliation.sql ────── UPDATE existing Prebid O&O rows → actual revenue
```

### Why this schedule?

| Source | Arrives | Step runs | Operation |
|---|---|---|---|
| `events` | Streaming (continuous) | Every hour (last 2 hours) | DELETE + INSERT |
| `ssp_report` | 8 AM UTC | 08:00–09:00 UTC | DELETE + INSERT |
| `syndication_revenue` | 8 AM UTC | 08:00–09:00 UTC | DELETE + INSERT |
| `gam_data_transfer` | ~4 PM UTC (within 8h) | 16:00–17:00 UTC | UPDATE only |
| `demand_partner_reports` | 9 AM UTC (late arriving) | 09:00–10:00 UTC | UPDATE only |

**Why 2-hour windows?** The events pipeline deletes and re-inserts the last 2 hours each run to handle late-arriving events. Reconciliation steps (GAM, demand partners) also run in 2-hour windows so they can re-apply actual revenue to any rows that were just re-inserted by the events pipeline.

**Idempotency:**
- The events pipeline only clears the last 2 hours — previously reconciled hours are never touched.
- SSP and syndication pipelines delete their specific event types (`ssp_external`, `syndication`) for the 3-day window before re-inserting, so they are safe to re-run.
- GAM and demand partner steps only UPDATE existing rows — no rows are deleted. They rely on the events pipeline having already inserted the base rows.

---

## Pipeline Steps

### Step 1 — Events → SSOT (`01_events_to_ssot.sql`)

**Runs:** Every hour (last 2 hours only)
**Type:** DELETE + INSERT

Deletes the current and previous hour from SSOT, then re-aggregates and inserts events for those 2 hours. This preserves reconciled data for earlier hours while handling late-arriving events.

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
- Revenue is distributed proportionally by impression count 
- GAM and MinuteSSP rows are excluded — they are reconciled by dedicated pipelines

---
