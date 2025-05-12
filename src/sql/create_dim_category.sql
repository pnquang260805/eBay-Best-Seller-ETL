CREATE TABLE IF NOT EXISTS dim_category(
    category_sk UInt16,
    category_id String,
    category_name String
)
ENGINE = MergeTree
ORDER BY (category_sk)
PRIMARY KEY(category_sk);