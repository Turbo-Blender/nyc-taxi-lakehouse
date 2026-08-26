CREATE OR REPLACE VIEW silver.yellow_trips AS
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
    airport_fee,

    EXTRACT(
        EPOCH FROM (dropoff_datetime - pickup_datetime)
    ) / 60 AS trip_duration_minutes,

    CASE
        WHEN EXTRACT(
            EPOCH FROM (dropoff_datetime - pickup_datetime)
        ) > 0
        THEN trip_distance /
             (
                 EXTRACT(
                     EPOCH FROM (dropoff_datetime - pickup_datetime)
                 ) / 3600
             )
        ELSE NULL
    END AS avg_speed_mph,

    CASE
        WHEN fare_amount > 0
        THEN tip_amount / fare_amount
        ELSE NULL
    END AS tip_percentage,

    pickup_datetime::date AS pickup_date,

    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,

    EXTRACT(DOW FROM pickup_datetime) AS pickup_day_of_week

FROM silver.yellow_trips_deduplicated;