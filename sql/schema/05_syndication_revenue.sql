-- Source: third-party syndication partners (Yahoo Sports, MSN, etc.)
-- External publishers embed our content and pay flat fees and/or revenue share
-- NOT tracked in events table — this is the only source for this revenue stream
-- NOTE (BigQuery): partition by transaction_date

CREATE TABLE IF NOT EXISTS syndication_revenue (
    transaction_date    TEXT    NOT NULL,   -- UTC date (YYYY-MM-DD)
    partner_name        TEXT    NOT NULL,
    content_property    TEXT    NOT NULL,
    article_count       INTEGER NOT NULL DEFAULT 0,
    flat_fee            REAL    NOT NULL DEFAULT 0.0,
    revenue_share       REAL    NOT NULL DEFAULT 0.0,
    total_revenue       REAL    NOT NULL DEFAULT 0.0
);
