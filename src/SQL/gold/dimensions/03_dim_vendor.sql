CREATE TABLE IF NOT EXISTS gold.dim_vendor
(
    vendor_key INT PRIMARY KEY,
    vendor_name VARCHAR(50) NOT NULL
);

INSERT INTO gold.dim_vendor
(vendor_key, vendor_name) VALUES
(0, 'Unknown'),
(1, 'Creative Mobile Technologies'),
(2, 'VeriFone Inc.')
ON CONFLICT (vendor_key) DO NOTHING;