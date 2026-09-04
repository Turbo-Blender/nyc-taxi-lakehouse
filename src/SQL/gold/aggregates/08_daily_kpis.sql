CREATE TABLE IF NOT EXISTS gold.daily_kpis (
    date_key INTEGER NOT NULL
        REFERENCES gold.dim_date(date_key),

    trip_count BIGINT NOT NULL,
    total_passengers BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    average_fare DOUBLE PRECISION,
    average_tip DOUBLE PRECISION,
    average_distance DOUBLE PRECISION,
    average_trip_duration DOUBLE PRECISION,
    average_speed_mph DOUBLE PRECISION,

    PRIMARY KEY (date_key)
);

TRUNCATE TABLE gold.daily_kpis;

INSERT INTO gold.daily_kpis (
    date_key,
    trip_count,
    total_passengers,
    total_revenue,
    average_fare,
    average_tip,
    average_distance,
    average_trip_duration,
    average_speed_mph
)
SELECT
    pickup_date_key AS date_key,
    COUNT(*) AS trip_count,
    SUM(passenger_count)::BIGINT AS total_passengers,
    COALESCE(SUM(total_amount), 0.0) AS total_revenue,
    AVG(fare_amount) AS average_fare,
    AVG(tip_amount) AS average_tip,
    AVG(trip_distance) AS average_distance,
    AVG(trip_duration_minutes) AS average_trip_duration,
    AVG(avg_speed_mph) AS average_speed_mph
FROM gold.fact_yellow_trips
GROUP BY pickup_date_key;