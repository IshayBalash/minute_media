-- Reference table: maps line item IDs to advertiser names
-- Used to populate the `advertiser` field in the SSOT

CREATE TABLE IF NOT EXISTS line_items_mapping (
    line_item_id    TEXT    NOT NULL PRIMARY KEY,
    advertiser      TEXT    NOT NULL
);
