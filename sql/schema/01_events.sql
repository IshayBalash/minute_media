CREATE TABLE IF NOT EXISTS events (
    timestamp       TEXT        NOT NULL,   
    event           TEXT        NOT NULL,   
    organization_id TEXT        NOT NULL,
    adunit          TEXT,
    media_type      TEXT,
    network         TEXT,
    cpm             REAL        NOT NULL DEFAULT 0.0,  
    sessionid       TEXT,
    url             TEXT,
    line_item       TEXT,
    ad_deal_type    TEXT,
    demand_owner    TEXT,
    paying_entity   TEXT,
    country         TEXT
);
