CREATE TABLE IF NOT EXISTS gold.dim_rate_code
(
    rate_code_key INT PRIMARY KEY,
    rate_code_name VARCHAR(100) NOT NULL
);

INSERT INTO gold.dim_rate_code (rate_code_key, rate_code_name)
VALUES
(-1, 'Unknown / missing'),
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Unknown')
ON CONFLICT (rate_code_key) DO UPDATE SET
    rate_code_name = EXCLUDED.rate_code_name;