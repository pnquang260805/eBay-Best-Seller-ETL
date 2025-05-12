CREATE TABLE IF NOT EXISTS dim_seller (
    seller_sk UInt16,
    seller_name String NOT NULL,
    feedback_percentage Decimal32(1),
    feedback_score UInt32
) ENGINE = MergeTree
ORDER BY (seller_sk)
PRIMARY KEY (seller_sk);