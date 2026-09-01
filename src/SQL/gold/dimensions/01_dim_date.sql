CREATE TABLE IF NOT EXISTS gold.dim_date
(
date_key INT PRIMARY KEY,
full_date DATE NOT NULL,
year INT NOT NULL,
quarter INT NOT NULL,
month INT NOT NULL,
month_name TEXT NOT NULL,
day INT NOT NULL,
day_of_week INT NOT NULL,
day_name TEXT NOT NULL,
is_weekend BOOLEAN NOT NULL
);

INSERT INTO gold.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    TO_CHAR(date_value, 'YYYYMMDD')::INT AS date_key,
    date_value AS full_date,
    EXTRACT(YEAR FROM date_value)::INT AS year,
    EXTRACT(QUARTER FROM date_value)::INT AS quarter,
    EXTRACT(MONTH FROM date_value)::INT AS month,
    TO_CHAR(date_value, 'FMMonth') AS month_name,
    EXTRACT(DAY FROM date_value)::INT AS day,
    EXTRACT(ISODOW FROM date_value)::INT AS day_of_week,
    TO_CHAR(date_value, 'FMDay') AS day_name,
    EXTRACT(ISODOW FROM date_value) IN (6, 7) AS is_weekend
FROM GENERATE_SERIES(
    (SELECT MIN(pickup_datetime)::DATE FROM silver.yellow_trips),
    (SELECT MAX(dropoff_datetime)::DATE FROM silver.yellow_trips),
    INTERVAL '1 day'
) AS dates(date_value)
ON CONFLICT (date_key) DO NOTHING;

