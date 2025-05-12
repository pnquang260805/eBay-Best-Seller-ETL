CREATE TABLE IF NOT EXISTS dim_item(
    item_sk UInt16,
    item_id String,
    title String,
    condition String,
    adult_only UInt8,
    item_location String,
    item_origin_date Date,
    item_creation_date Date
) ENGINE = MergeTree
ORDER BY (item_sk)
PRIMARY KEY (item_sk);