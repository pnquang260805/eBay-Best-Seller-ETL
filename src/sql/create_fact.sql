CREATE TABLE IF NOT EXISTS eBay_best_seller_fact(
    date_key String NOT NULL,
    item_sk Int16,
    seller_sk Int16,
    price Decimal64(2),
    top_rated_buying_experience UInt8,
    listing_marketplace_id String
) ENGINE = MergeTree
ORDER BY (date_key, item_sk, seller_sk)
PRIMARY KEY (date_key, item_sk, seller_sk);