CREATE TABLE IF NOT EXISTS gold.zone_hourly_demand (
    pickup_zone_key INTEGER NOT NULL
        REFERENCES gold.dim_taxi_zone(zone_key),

    pickup_date_key INTEGER NOT NULL
        REFERENCES gold.dim_date(date_key),

    pickup_hour SMALLINT NOT NULL,

    trip_count BIGINT NOT NULL,
    total_passengers BIGINT NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    average_fare DOUBLE PRECISION,
    average_distance DOUBLE PRECISION,
    average_trip_duration DOUBLE PRECISION,

    PRIMARY KEY (
        pickup_zone_key,
        pickup_date_key,
        pickup_hour
    )
);

TRUNCATE TABLE gold.zone_hourly_demand;

INSERT INTO gold.zone_hourly_demand (
    pickup_zone_key,
    pickup_date_key,
    pickup_hour,
    trip_count,
    total_passengers,
    total_revenue,
    average_fare,
    average_distance,
    average_trip_duration
)
SELECT
    pickup_zone_key,
    pickup_date_key,
    EXTRACT(HOUR FROM pickup_datetime)::SMALLINT AS pickup_hour,
    COUNT(*) AS trip_count,
    SUM(passenger_count)::BIGINT AS total_passengers,
    COALESCE(SUM(total_amount), 0.0) AS total_revenue,
    AVG(fare_amount) AS average_fare,
    AVG(trip_distance) AS average_distance,
    AVG(trip_duration_minutes) AS average_trip_duration
FROM gold.fact_yellow_trips
GROUP BY
    pickup_zone_key,
    pickup_date_key,
    EXTRACT(HOUR FROM pickup_datetime)::SMALLINT;