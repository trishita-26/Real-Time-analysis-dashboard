-- ============================================
-- REAL TIME ANALYSIS DASHBOARD
-- SQL Cleaning Script
-- ============================================

-- Step 1: Create raw_data table
CREATE TABLE raw_data (
    id             SERIAL PRIMARY KEY,
    order_id       INT,
    user_id        INT,
    product_id     INT,
    product_name   VARCHAR(100),
    category       VARCHAR(50),
    price          INT,
    quantity       INT,
    event_type     VARCHAR(20),
    timestamp      VARCHAR(30),
    payment_method VARCHAR(20),
    city           VARCHAR(50)
);


SELECT COUNT(*) FROM raw_data;

SELECT * FROM raw_data LIMIT 10;

-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- check 1: checking null values

SELECT COUNT(*) FROM raw_data
WHERE order_id IS NULL
   OR price <= 0
   OR quantity <= 0
   OR event_type IS NULL
   OR product_name IS NULL
   OR city IS NULL;
     
SELECT 
    COUNT(*) FILTER (WHERE user_id IS NULL)        AS null_user_id,
    COUNT(*) FILTER (WHERE product_id IS NULL)     AS null_product_id,
    COUNT(*) FILTER (WHERE category IS NULL)       AS null_category,
    COUNT(*) FILTER (WHERE payment_method IS NULL) AS null_payment_method,
    COUNT(*) FILTER (WHERE timestamp IS NULL)      AS null_timestamp
FROM raw_data;  


--Check 2: Duplicate orders

SELECT order_id, COUNT(*) as count
FROM raw_data
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10;

--Check 3: True duplicates checking
SELECT order_id, product_id, product_name, price, quantity
FROM raw_data
WHERE order_id = 1234
LIMIT 10;

SELECT 
    order_id, 
    product_id, 
    price, 
    quantity, 
    COUNT(*) as count
FROM raw_data
GROUP BY order_id, product_id, price, quantity
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10;


-- Check 4: True duplicates with timestamp
SELECT 
    order_id,
    product_id,
    price,
    quantity,
    timestamp,
    COUNT(*) as count
FROM raw_data
GROUP BY order_id, product_id, price, quantity, timestamp
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10;

-- Check 5: Valid event types

SELECT event_type, COUNT(*) as count
FROM raw_data
GROUP BY event_type;

-- Check 6: Price and quantity ranges
SELECT 
    MIN(price)    AS min_price,
    MAX(price)    AS max_price,
    MIN(quantity) AS min_quantity,
    MAX(quantity) AS max_quantity
FROM raw_data;

-- Check 7: checking category distribution
SELECT category, COUNT(*) as count
FROM raw_data
GROUP BY category
ORDER BY count DESC;

-- Check 8: Timestamp format and length
-- timestamp check
SELECT 
    timestamp,
    LENGTH(timestamp) as length
FROM raw_data
LIMIT 10;

-- checking invalid timestamp
SELECT COUNT(*) 
FROM raw_data
WHERE timestamp IS NULL
   OR LENGTH(timestamp) < 10
   OR timestamp = '';

--checking are all timestamp recent
SELECT 
    MIN(timestamp) AS oldest,
    MAX(timestamp) AS newest
FROM raw_data;

SELECT COUNT(*)
FROM raw_data
WHERE timestamp < '2026-01-01'
   OR timestamp > '2026-12-31';

-- format checking
SELECT 
    COUNT(*) FILTER (WHERE timestamp LIKE '%T%') AS iso_format,
    COUNT(*) FILTER (WHERE timestamp LIKE '% %') AS normal_format,
    COUNT(*) FILTER (WHERE timestamp NOT LIKE '%T%' 
                    AND timestamp NOT LIKE '% %') AS unknown_format
FROM raw_data;

-- Check 10: City distribution
-- checking all 4 valid cities present or not
SELECT city, COUNT(*) as count
FROM raw_data
GROUP BY city
ORDER BY count DESC;

-- Check 11: Payment method distribution
-- checking all valid payment method is present
SELECT payment_method, COUNT(*) as count
FROM raw_data
GROUP BY payment_method
ORDER BY count DESC;
   

-- ============================================
-- CREATE CLEANED TABLE
-- ============================================

CREATE TABLE cleaned_data AS
SELECT 
    order_id,
    user_id,
    product_id,
    product_name,
    category,
    price,
    quantity,
    event_type,
    timestamp,
    payment_method,
    city,
    price * quantity AS revenue
FROM raw_data;


-- ============================================
-- VERIFY COUNTS
-- ============================================

SELECT COUNT(*) FROM raw_data;
SELECT COUNT(*) FROM cleaned_data;