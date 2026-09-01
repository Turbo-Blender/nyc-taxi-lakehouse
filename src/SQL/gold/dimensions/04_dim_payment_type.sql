CREATE TABLE IF NOT EXISTS gold.dim_payment_type
(
    payment_type_key INT PRIMARY KEY,
    payment_type_name VARCHAR(100) NOT NULL
);

INSERT INTO gold.dim_payment_type (payment_type_key, payment_type_name)
VALUES
    (-1, 'Unknown / missing'),
    (0, 'Flex Fare trip'),
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
ON CONFLICT (payment_type_key) DO UPDATE SET
    payment_type_name = EXCLUDED.payment_type_name;
