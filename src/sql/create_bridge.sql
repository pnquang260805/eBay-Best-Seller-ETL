CREATE TABLE IF NOT EXISTS bridge_item_category(
    item_sk Int16,
    category_sk Int16
) ENGINE = MergeTree
ORDER BY (item_sk);