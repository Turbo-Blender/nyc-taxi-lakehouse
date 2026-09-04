CREATE TABLE IF NOT EXISTS gold.dim_taxi_zone
(
    zone_key INT PRIMARY KEY,
    borough TEXT NOT NULL,
    zone_name TEXT NOT NULL,
    service_zone TEXT
);

INSERT INTO gold.dim_taxi_zone 
(
    zone_key,
    borough,
    zone_name,
    service_zone
)
SELECT
location_id AS zone_key,
borough,
zone_name,
service_zone
FROM reference.taxi_zones
ON CONFLICT (zone_key) DO UPDATE SET
    borough = EXCLUDED.borough,
    zone_name = EXCLUDED.zone_name,
    service_zone = EXCLUDED.service_zone;

INSERT INTO gold.dim_taxi_zone (
    zone_key,
    borough,
    zone_name,
    service_zone
)
VALUES (
    0,
    'Unknown',
    'Unknown',
    'Unknown'
)
ON CONFLICT (zone_key) DO NOTHING;
