CREATE OR REPLACE VIEW silver.yellow_trips_deduplicated AS
SELECT *
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