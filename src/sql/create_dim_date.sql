CREATE TABLE IF NOT EXISTS dim_date (
    date_key VARCHAR(10) NOT NULL,
    year Int16,
    month Int16,
    day Int16,
    day_of_week String,
    quarter Int16
) ENGINE = MergeTree
ORDER BY (date_key)
PRIMARY KEY(date_key);