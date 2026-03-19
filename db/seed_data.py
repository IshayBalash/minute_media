"""
Seed the database with sample data from the assignment.
Run this once to populate all source tables before running the pipeline.
Safe to re-run — all tables are truncated before inserting.
"""

from db.manager import DBManager
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

EVENTS = [
    # (timestamp, event, organization_id, adunit, media_type, network, cpm, sessionid, url, line_item, ad_deal_type, demand_owner, paying_entity, country)
    ("2024-01-15 08:23:41", "served",     "si",          "/23162276296/.../tout_d",    "banner", "GAM",       0.04,  "lhuWTd",  "si.com/mlb/cubs-sign-alex",           "6756746583", "rtb",    "SI",        "Adx",          "US"),
    ("2024-01-15 08:23:42", "served",     "90min",       "/12345/.../article_top",     "banner", "GAM",       1.20,  "kf83jd",  "90min.com/posts/transfer-news",        "5643454502", "direct", "SI",        "direct",       "GB"),
    ("2024-01-15 08:23:43", "served",     "publisher_x", "/pub-x-123/homepage",        "banner", "Prebid",    1.20,  "abc123",  "publisherx.com/sports/home",           "7890123456", "rtb",    "SI",        "Magnite",      "CA"),
    ("2024-01-15 08:23:44", "served",     "si",          "video_player_main",          "video",  "GAM",      11.00,  "xYz789",  "si.com/nfl/playoff-preview",           "8901234567", "direct", "SI",        "direct",       "US"),
    ("2024-01-15 08:23:45", "served",     "publisher_y", "video_player_embed",         "video",  "MinuteSSP",12.00,  "mno456",  "publishery.com/video/sports",          None,         "rtb",    "SI",        "MinuteSSP",    "DE"),
    ("2024-01-15 08:23:46", "pageView",   "si",          None,                         None,     None,        0.00,  "lhuWTd",  "si.com/mlb/cubs-sign-alex",            None,         None,     "SI",        None,           "US"),
    ("2024-01-15 08:23:47", "served",     "fs",          "/987654/.../sidebar",        "banner", "Prebid",    1.80,  "def456",  "fansided.com/nba/lakers-trade",        "4567890123", "rtb",    "SI",        "Triplelift",   "US"),
    ("2024-01-15 08:23:48", "videoEmbed", "publisher_y", "video_player_embed",         "video",  None,        0.00,  "mno456",  "publishery.com/video/sports",          None,         None,     "SI",        None,           "DE"),
    ("2024-01-15 08:23:49", "served",     "publisher_x", "/pub-x-123/sidebar",         "banner", "Prebid",    0.95,  "pqr123",  "publisherx.com/sports/article",        "3456789012", "rtb",    "PublisherX", "IndexExchange","MX"),
    ("2024-01-15 08:23:50", "served",     "90min",       "/12345/.../video_pre",       "video",  "GAM",      12.00,  "yzA789",  "90min.com/videos/highlights",          "6789012345", "rtb",    "SI",        "Adx",          "FR"),
]

SSP_REPORT = [
    # (report_date, publisher_id, placement_type, site_type, country_code, impressions, revenue_usd)
    ("2024-01-15", "si",           "display", "own_site",   "USA",  45000,   4050.00),
    ("2024-01-15", "publisher_x",  "display", "external",   "CAN", 120000, 14400.00),
    ("2024-01-15", "si",           "video",   "own_player", "USA",   8000, 120000.00),
    ("2024-01-15", "publisher_y",  "video",   "own_player", "DEU",  15000, 180000.00),
    ("2024-01-15", "external_pub1","video",   "ext_player", "GBR",  50000, 500000.00),
    ("2024-01-15", "external_pub2","display", "external",   "MEX", 200000,  20000.00),
]

GAM_DATA_TRANSFER = [
    # (timestamp, session_id, ad_unit_id, line_item_id, country_name, impressions, cpm_usd)
    # Note: session_id is lowercase in GAM (matches events.sessionid lowercased)
    ("2024-01-15 08:23:41", "lhuwtd", "123456789", "6756746583", "United States",  1,  0.09),
    ("2024-01-15 08:23:42", "kf83jd", "234567890", "5643454502", "United Kingdom", 1,  2.50),
    ("2024-01-15 08:23:44", "xyz789", "345678901", "8901234567", "United States",  1, 15.00),
    ("2024-01-15 08:23:50", "yza789", "456789012", "6789012345", "France",         1, 18.00),
]

DEMAND_PARTNER_REPORTS = [
    # (report_date, partner_name, property_code, ad_unit, geo, impressions, revenue_usd)
    ("2024-01-15", "Magnite",       "si",    None,              "us",  850000,  85000.00),
    ("2024-01-15", "Triplelift",    "si",    "homepage_top",    "us",  320000,  45000.00),
    ("2024-01-15", "Triplelift",    "90min", "article_sidebar", "gb",  480000,  67200.00),
    ("2024-01-15", "IndexExchange", "si",    None,              "us",  920000,  73600.00),
    ("2024-01-15", "IndexExchange", "90min", None,              "fr", 1100000,  99000.00),
]

SYNDICATION_REVENUE = [
    # (transaction_date, partner_name, content_property, article_count, flat_fee, revenue_share, total_revenue)
    ("2024-01-15", "Yahoo Sports", "90min",    150, 5000.00, 2500.00, 7500.00),
    ("2024-01-15", "MSN",          "FanSided",  80,    0.00, 1800.00, 1800.00),
]

# Maps line item IDs to advertiser names.
# In production this table is maintained by the ad ops team.
LINE_ITEMS_MAPPING = [
    # (line_item_id, advertiser)
    ("6756746583", "Nike"),
    ("5643454502", "Adidas"),
    ("7890123456", "ESPN"),
    ("8901234567", "Gatorade"),
    ("4567890123", "DraftKings"),
    ("3456789012", "FanDuel"),
    ("6789012345", "Puma"),
]


# ---------------------------------------------------------------------------
# Seed function
# ---------------------------------------------------------------------------

def seed(db: DBManager) -> None:
    """Create all tables, truncate existing data, and insert fresh sample data."""
    logger.info("Creating tables if not exsist")

    # --- Schema ---
    schema_files = [
        "sql/schema/01_events.sql",
        "sql/schema/02_ssp_report.sql",
        "sql/schema/03_gam_data_transfer.sql",
        "sql/schema/04_demand_partner_reports.sql",
        "sql/schema/05_syndication_revenue.sql",
        "sql/schema/06_line_items_mapping.sql",
        "sql/schema/07_ssot.sql",
    ]
    for f in schema_files:
        db.execute_file(f)

    # --- Truncate all tables before inserting ---
    tables = [
        "events",
        "ssp_report",
        "gam_data_transfer",
        "demand_partner_reports",
        "syndication_revenue",
        "line_items_mapping",
        "ssot",
    ]
    logger.info("Truncating existing data from all tables.")
    for table in tables:
        db.execute(f"DELETE FROM {table}")

    # --- events ---
    logger.info("Inserting sample data to tables...")
    db.execute_many(
        """
        INSERT INTO events (
            timestamp, event, organization_id, adunit, media_type, network,
            cpm, sessionid, url, line_item, ad_deal_type, demand_owner, paying_entity, country
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        EVENTS,
    )

    # --- ssp_report ---
    db.execute_many(
        """
        INSERT INTO ssp_report (
            report_date, publisher_id, placement_type, site_type, country_code, impressions, revenue_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        SSP_REPORT,
    )

    # --- gam_data_transfer ---
    db.execute_many(
        """
        INSERT INTO gam_data_transfer (
            timestamp, session_id, ad_unit_id, line_item_id, country_name, impressions, cpm_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        GAM_DATA_TRANSFER,
    )

    # --- demand_partner_reports ---
    db.execute_many(
        """
        INSERT INTO demand_partner_reports (
            report_date, partner_name, property_code, ad_unit, geo, impressions, revenue_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        DEMAND_PARTNER_REPORTS,
    )

    # --- syndication_revenue ---
    db.execute_many(
        """
        INSERT INTO syndication_revenue (
            transaction_date, partner_name, content_property, article_count,
            flat_fee, revenue_share, total_revenue
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        SYNDICATION_REVENUE,
    )

    # --- line_items_mapping ---
    db.execute_many(
        "INSERT INTO line_items_mapping (line_item_id, advertiser) VALUES (?, ?)",
        LINE_ITEMS_MAPPING,
    )

    logger.info("Done. Database is ready.")

