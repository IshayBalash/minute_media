CREATE TABLE IF NOT EXISTS ssot (
    date            TEXT    NOT NULL,  
    rounded_hour    INTEGER,            
    event           TEXT    NOT NULL,  
    organization_id TEXT    NOT NULL,
    adunit          TEXT,
    media_type      TEXT,
    network         TEXT,
    domain          TEXT,              
    line_item       TEXT,
    advertiser      TEXT,          
    ad_deal_type    TEXT,
    demand_owner    TEXT,
    country         TEXT,
    revenue         REAL    NOT NULL DEFAULT 0.0, 
    event_count     INTEGER NOT NULL DEFAULT 0
);
