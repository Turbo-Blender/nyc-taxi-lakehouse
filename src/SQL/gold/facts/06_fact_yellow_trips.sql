CREATE TABLE IF NOT EXISTS gold.fact_yellow_trips (
    trip_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    pickup_date_key INTEGER NOT NULL
        REFERENCES gold.dim_date(date_key),

    dropoff_date_key INTEGER NOT NULL
        REFERENCES gold.dim_date(date_key),

    pickup_zone_key INTEGER NOT NULL
        REFERENCES gold.dim_taxi_zone(zone_key),

    dropoff_zone_key INTEGER NOT NULL
        REFERENCES gold.dim_taxi_zone(zone_key),

    vendor_key INTEGER NOT NULL
        REFERENCES gold.dim_vendor(vendor_key),

    payment_type_key INTEGER NOT NULL
        REFERENCES gold.dim_payment_type(payment_type_key),

    rate_code_key INTEGER NOT NULL
        REFERENCES gold.dim_rate_code(rate_code_key),

    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    store_and_forward_flag TEXT,

    passenger_count BIGINT NOT NULL,
    trip_distance DOUBLE PRECISION NOT NULL,
    trip_duration_minutes DOUBLE PRECISION,

    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,

    avg_speed_mph DOUBLE PRECISION,
    tip_percentage DOUBLE PRECISION
);

TRUNCATE TABLE gold.fact_yellow_trips RESTART IDENTITY;

INSERT INTO gold.fact_yellow_trips (
    pickup_date_key,
    dropoff_date_key,
    pickup_zone_key,
    dropoff_zone_key,
    vendor_key,
    payment_type_key,
    rate_code_key,
    pickup_datetime,
    dropoff_datetime,
    store_and_forward_flag,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    extra,
    mta_tax,
    improvement_surcharge,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    airport_fee,
    avg_speed_mph,
    tip_percentage
)
SELECT
    pickup_date.date_key,
    dropoff_date.date_key,

    COALESCE(pickup_zone.zone_key, 0),
    COALESCE(dropoff_zone.zone_key, 0),
    COALESCE(vendor.vendor_key, 0),
    COALESCE(payment.payment_type_key, -1),
    COALESCE(rate_code.rate_code_key, -1),

    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_forward_flag,
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_duration_minutes,
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.improvement_surcharge,
    trips.tip_amount,
    trips.tolls_amount,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.avg_speed_mph,
    trips.tip_percentage

FROM silver.yellow_trips AS trips

JOIN gold.dim_date AS pickup_date
    ON pickup_date.full_date = trips.pickup_datetime::DATE

JOIN gold.dim_date AS dropoff_date
    ON dropoff_date.full_date = trips.dropoff_datetime::DATE

LEFT JOIN gold.dim_taxi_zone AS pickup_zone
    ON pickup_zone.zone_key = trips.pickup_location_id

LEFT JOIN gold.dim_taxi_zone AS dropoff_zone
    ON dropoff_zone.zone_key = trips.dropoff_location_id

LEFT JOIN gold.dim_vendor AS vendor
    ON vendor.vendor_key = trips.vendor_id

LEFT JOIN gold.dim_payment_type AS payment
    ON payment.payment_type_key = trips.payment_type

LEFT JOIN gold.dim_rate_code AS rate_code
    ON rate_code.rate_code_key = trips.rate_code_id;