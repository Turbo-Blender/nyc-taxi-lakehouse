
CREATE OR REPLACE VIEW silver.yellow_trips_deduplicated AS
SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    rate_code_id,
    store_and_forward_flag,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    improvement_surcharge,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    airport_fee
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                total_amount
            ORDER BY pickup_datetime
        ) AS rn
    FROM silver.yellow_trips_cleaned
)
WHERE rn = 1;