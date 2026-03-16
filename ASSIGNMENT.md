# Data Engineer Home Assignment — Minute Media (BI & Data Team)

## Overview

This assignment simulates a real-world scenario: building a revenue **Single Source of Truth (SSOT)** for a multi-business-model company. You'll consolidate data from multiple revenue streams and design scalable pipelines for high-volume event data.

---

## Background & Context

### Our Business Model

**1. Display Monetization** (banners, pop-ups, etc.)
- **O&O (Owned & Operated):** Monetizing our own properties (Sports Illustrated `si`, 90min, FanSided `fs`). Multiple web-sites can be consolidated under one property.
- **B2B Publishers:** Providing monetization solutions to external publishers
- Both run programmatic and direct campaigns
- All events tracked in BigQuery `events` table (Display ad impression → `served` event)
- CPM values are estimated in real time and subject to reconciliation, **except MinuteSSP which is real CPM**

**2. Video Player** (video content and ads)
- **O&O:** Video player embedded on our sites
- **B2B Publishers:** Video player embedded on external publisher sites
- Both run programmatic and direct video ads
- All events tracked in BigQuery `events` table (`served` for ad impressions, `videoEmbed` for player embed)
- CPM values are estimated in real time and subject to reconciliation, **except MinuteSSP which is real CPM**

**3. SSP (Supply-Side Platform)**
- We operate our own SSP that participates in programmatic auctions
- SSP activity reported in BigQuery `ssp_report` table
- Our SSP can run on:
  - Our own display monetization solution (own sites or external sites) → also in `events`
  - Our own video player (own sites or external sites) → also in `events`
  - **External display monetization** (other companies' solutions on external sites) → **NOT in `events`**
  - **External video players** (other companies' players on external sites) → **NOT in `events`**
- When our SSP wins on our own sites/player/display solution, it's also captured in `events` with better granularity (revenue is exactly the same)

**4. Content Syndication**
- External publishers embed our content on their sites (Yahoo Sports, MSN, etc.)
- External publishers pay flat fees and/or revenue share → called **"syndication partner revenue"**
- Data provided by 3rd party only (`syndication_revenue`) → **NOT tracked in `events`**

---

## The Data Sources

### Table 1: `events` — First-Party Event Level Raw Data

Receives **billions of rows per day**. Captures all activity on our sites and B2B publisher integrations. **Retains only last 60 days.**

| Column | Description |
|---|---|
| `timestamp` | Event occurrence time (UTC) |
| `event` | Type: `pageView`, `served`, `videoEmbed` |
| `organizationId` | Unique identifier for the property/publisher |
| `adunit` | Ad unit identifier where the ad was served |
| `mediaType` | Format of the ad placement (banner, video) |
| `network` | Ad network used (GAM, Prebid, MinuteSSP, etc.) |
| `cpm` | Revenue per thousand impressions (USD). Estimated in real time, subject to reconciliation |
| `sessionid` | Unique identifier for a user's browsing session |
| `url` | Full page URL where the event occurred |
| `lineItem` | Unique identifier for ad delivery rules/settings |
| `adDealType` | Type of advertising deal structure (rtb, direct) |
| `demandOwner` | Entity that owns the advertising demand relationship |
| `payingEntity` | Ultimate entity responsible for payment |
| `country` | User's geographic location |

**Sample data:**

| Timestamp (UTC) | event | organizationId | adunit | mediaType | network | cpm | sessionid | url | lineItem | adDealType | demandOwner | payingEntity | country |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2024-01-15 08:23:41 | served | si | /23162276296/.../tout_d | banner | GAM | 0.04 | lhuWTd... | si.com/mlb/cubs-sign-alex... | 6756746583 | rtb | SI | Adx | US |
| 2024-01-15 08:23:42 | served | 90min | /12345/.../article_top | banner | GAM | 1.20 | kf83jd... | 90min.com/posts/transfer-news | 5643454502 | direct | SI | direct | GB |
| 2024-01-15 08:23:43 | served | publisher_x | /pub-x-123/homepage | banner | Prebid | 1.20 | abc123... | publisherx.com/sports/home | 7890123456 | rtb | SI | Magnite | CA |
| 2024-01-15 08:23:44 | served | si | video_player_main | video | GAM | 11.0 | xYz789... | si.com/nfl/playoff-preview | 8901234567 | direct | SI | direct | US |
| 2024-01-15 08:23:45 | served | publisher_y | video_player_embed | video | MinuteSSP | 12.0 | mno456... | publishery.com/video/sports | NULL | rtb | SI | MinuteSSP | DE |
| 2024-01-15 08:23:46 | pageView | si | NULL | NULL | NULL | 0.00 | lhuWTd... | si.com/mlb/cubs-sign-alex... | NULL | NULL | SI | NULL | US |
| 2024-01-15 08:23:47 | served | fs | /987654/.../sidebar | banner | Prebid | 1.80 | def456... | fansided.com/nba/lakers-trade | 4567890123 | rtb | SI | Triplelift | US |
| 2024-01-15 08:23:48 | videoEmbed | publisher_y | video_player_embed | video | | 0.00 | mno456... | publishery.com/video/sports | | | SI | | DE |
| 2024-01-15 08:23:49 | served | publisher_x | /pub-x-123/sidebar | banner | Prebid | 0.95 | pqr123... | publisherx.com/sports/article | 3456789012 | rtb | PublisherX | IndexExchange | MX |
| 2024-01-15 08:23:50 | served | 90min | /12345/.../video_pre | video | GAM | 12.0 | yzA789... | 90min.com/videos/highlights | 6789012345 | rtb | SI | Adx | FR |

> **Additional reference table:** `line_items_mapping` — Maps line item IDs to advertisers.

---

### Table 2: `ssp_report` — Third-Party SSP Platform Report

Daily aggregated report from our SSP infrastructure. Contains **ALL SSP activity** including external players/sites not directly tracked.

| Column | Description |
|---|---|
| `report_date` | Date (UTC) |
| `publisher_id` | Publisher identifier |
| `placement_type` | `display` or `video` |
| `site_type` | `own_site`, `external`, `own_player`, `ext_player` |
| `country_code` | ISO country code |
| `impressions` | Total impressions |
| `revenue_usd` | Revenue in USD |

**Sample data:**

| Report_date | publisher_id | placement_type | site_type | country_code | impressions | revenue_usd |
|---|---|---|---|---|---|---|
| 2024-01-15 | si | display | own_site | USA | 45,000 | 4,050.00 |
| 2024-01-15 | publisher_x | display | external | CAN | 120,000 | 14,400.00 |
| 2024-01-15 | si | video | own_player | USA | 8,000 | 120,000.00 |
| 2024-01-15 | publisher_y | video | own_player | DEU | 15,000 | 180,000.00 |
| 2024-01-15 | external_pub1 | video | ext_player | GBR | 50,000 | 500,000.00 |
| 2024-01-15 | external_pub2 | display | external | MEX | 200,000 | 20,000.00 |

---

### Table 3: `gam_data_transfer` — Google Ad Manager Data Transfer

Since CPM in real time is estimated, GAM provides Data Transfer tables with impression-level data **arriving within 8 hours**. Contains **actual CPMs when network is GAM** — used for CPM reconciliation with O&O properties.

| Column | Description |
|---|---|
| `timestamp` | Event time (UTC) |
| `session_id` | Session identifier (lowercase) |
| `ad_unit_id` | Ad unit identifier |
| `line_item_id` | Line item identifier |
| `country_name` | Country name |
| `impressions` | Number of impressions |
| `cpm_usd` | Actual CPM in USD |

**Sample data:**

| Timestamp (UTC) | session_id | ad_unit_id | line_item_id | country_name | impressions | cpm_usd |
|---|---|---|---|---|---|---|
| 2024-01-15 08:23:41 | lhuwtdltpsrvclba | 123456789 | 6756746583 | United States | 1 | 0.09 |
| 2024-01-15 08:23:42 | kf83jdkslf93jdk3 | 234567890 | 5643454502 | United Kingdom | 1 | 2.50 |
| 2024-01-15 08:23:44 | xyz789uvw321rst4 | 345678901 | 8901234567 | United States | 1 | 15.00 |
| 2024-01-15 08:23:50 | yza789bcd123efg4 | 456789012 | 6789012345 | France | 1 | 18.00 |

---

### Table 4: `demand_partner_reports` — Multiple Revenue Partners

Daily reports from demand partners (Magnite, Triplelift, Index Exchange, etc.) with varying granularity. **Only O&O properties are reconciled against partner reports. Does not contain GAM data.**

| Column | Description |
|---|---|
| `report_date` | Date (UTC) |
| `partner_name` | Partner name |
| `property_code` | Property identifier |
| `ad_unit` | Ad unit (can be NULL) |
| `geo` | Geography code |
| `impressions` | Total impressions |
| `revenue_usd` | Revenue in USD |

**Sample data:**

| Report_date | partner_name | property_code | ad_unit | geo | impressions | revenue_usd |
|---|---|---|---|---|---|---|
| 2024-01-15 | Magnite | si | NULL | us | 850,000 | 85,000.00 |
| 2024-01-15 | Triplelift | si | homepage_top | us | 320,000 | 45,000.00 |
| 2024-01-15 | Triplelift | 90min | article_sidebar | gb | 480,000 | 67,200.00 |
| 2024-01-15 | IndexExchange | si | NULL | us | 920,000 | 73,600.00 |
| 2024-01-15 | IndexExchange | 90min | NULL | fr | 1,100,000 | 99,000.00 |

---

### Table 5: `syndication_revenue`

Provided by 3rd party only — **not tracked in `events` table**.

| Column | Description |
|---|---|
| `transaction_date` | Date (UTC) |
| `partner_name` | Partner name |
| `content_property` | Content property |
| `article_count` | Number of articles |
| `flat_fee` | Flat fee portion |
| `revenue_share` | Revenue share portion |
| `total_revenue` | Total revenue |

**Sample data:**

| Transaction_date | partner_name | content_property | article_count | flat_fee | revenue_share | total_revenue |
|---|---|---|---|---|---|---|
| 2024-01-15 | Yahoo Sports | 90min | 150 | 5,000.00 | 2,500.00 | 7,500.00 |
| 2024-01-15 | MSN | FanSided | 80 | 0.00 | 1,800.00 | 1,800.00 |

---

## Your Assignment

Design a pipeline that builds a **granular revenue Single Source of Truth (SSOT)** that consolidates all revenue streams and enables comprehensive reporting and analysis. This data should contain the **adjusted revenue** and all events tracked.

### SSOT Table — Required Fields

| Field | Description |
|---|---|
| `date` | Date of the event |
| `rounded_hour` | Hour (rounded) |
| `event` | Event type |
| `organization_id` | Organization / publisher |
| `adunit` | Ad unit |
| `media_type` | Media type |
| `network` | Ad network |
| `domain` | Domain |
| `line_item` | Line item ID |
| `advertiser` | Advertiser name |
| `ad_deal_type` | Deal type |
| `demand_owner` | Demand owner |
| `country` | Country |
| `revenue` | Adjusted revenue (USD) |
| `event_count` | Count of events |

> **Deliverables:** Provide all code, schema designs, architecture diagrams, pipeline designs, data quality frameworks, and document all assumptions made in your approach.

---

## Additional Context

### Data Volumes & Retention

| Source | Volume | Retention |
|---|---|---|
| Events table | Billions of rows/day | Last 60 days only |
| SSOT (daily aggregated) | — | Full history retained |

### Data Arrival Times

| Source | Arrival Time |
|---|---|
| Events | Streaming (continuous) |
| GAM Data Transfer | Within 8 hours (~4 PM for previous day) |
| SSP report | 8 AM daily |
| Demand partner reports | 9 AM – next day (varies) |
| **Business requirement** | **SSOT needed by 10 AM daily for the previous day** |

---

Questions? [eran.hornik@minutemedia.com](mailto:eran.hornik@minutemedia.com)

*Good luck! We're looking forward to seeing your approach to this challenge.*
