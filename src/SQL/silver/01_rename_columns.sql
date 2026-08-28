CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.yellow_trips_renamed AS
SELECT
    "VendorID" AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    "RatecodeID" AS rate_code_id,
    store_and_fwd_flag AS store_and_forward_flag,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    improvement_surcharge,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    "Airport_fee" AS airport_fee
FROM raw.yellow_trips;
