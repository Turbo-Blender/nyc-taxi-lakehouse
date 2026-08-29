CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.taxi_zones (
    location_id INTEGER PRIMARY KEY,
    borough TEXT NOT NULL,
    zone_name TEXT NOT NULL,
    service_zone TEXT
);