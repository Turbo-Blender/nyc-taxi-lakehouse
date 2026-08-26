CREATE OR REPLACE VIEW silver.yellow_trips_cleaned AS 
SELECT * FROM silver.yellow_trips_renamed
WHERE
1=1
AND dropoff_datetime > pickup_datetime
AND passenger_count > 0
AND trip_distance > 0
AND total_amount >= 0;