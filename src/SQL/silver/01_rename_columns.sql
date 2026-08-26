CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.yellow_trips_renamed AS
SELECT
    "VendorID" AS vendor_id,
    "tpep_pickup_datetime" AS pickup_datetime,
    "tpep_dropoff_datetime" AS dropoff_datetime,
    "Passenger_count" AS passenger_count,
    "Trip_distance" AS trip_distance,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    "RateCodeID" AS rate_code_id,
    "Store_and_fwd_flag" AS store_and_forward_flag,
    "Payment_type" AS payment_type,
    "Fare_amount" AS fare_amount,
    "Extra" AS extra,
    "MTA_tax" AS mta_tax,
    "Improvement_surcharge" AS improvement_surcharge,
    "Tip_amount" AS tip_amount,
    "Tolls_amount" AS tolls_amount,
    "Total_amount" AS total_amount,
    "Congestion_Surcharge" AS congestion_surcharge,
    "Airport_fee" AS airport_fee
FROM raw.yellow_trips;