CREATE TABLE IF NOT EXISTS dim_date (
    date_key VARCHAR(10) NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week VARCHAR(100),
    quarter INTEGER,
    PRIMARY KEY(date_key)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_sk SERIAL,
    seller_name VARCHAR(255) NOT NULL,
    feedback_percentage DECIMAL(5, 1),
    feedback_score BIGINT,
    PRIMARY KEY (seller_sk)
);

CREATE TABLE IF NOT EXISTS dim_category(
    category_sk SERIAL,
    category_id VARCHAR(255),
    category_name VARCHAR(255),

    PRIMARY KEY(category_sk)
);

CREATE TABLE IF NOT EXISTS dim_item(
    item_sk SERIAL,
    item_id VARCHAR(20),
    title VARCHAR(255),
    condition VARCHAR(255),
    adult_only BOOLEAN,
    item_location VARCHAR(100),
    item_origin_date DATE,
    item_creation_date DATE,
    PRIMARY KEY (item_sk)
);

CREATE TABLE IF NOT EXISTS eBay_best_seller_fact(
    date_key VARCHAR(10) NOT NULL,
    item_sk INTEGER,
    seller_sk INTEGER,
    price DECIMAL(10, 2),
    top_rated_buying_experience BOOLEAN,
    listing_marketplace_id VARCHAR(20),

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (item_sk) REFERENCES dim_item(item_sk),
    FOREIGN KEY (seller_sk) REFERENCES dim_seller(seller_sk)
);