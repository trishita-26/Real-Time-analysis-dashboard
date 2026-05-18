-- ============================================
-- REAL TIME ANALYSIS DASHBOARD
-- FINAL DATA CLEANING SCRIPT
-- ============================================

DROP TABLE IF EXISTS cleaned_data;

CREATE TABLE cleaned_data AS

SELECT DISTINCT

    -- ============================================
    -- ORDER DETAILS
    -- ============================================

    order_id,

    COALESCE(user_id, -1) AS user_id,

    product_id,



    -- ============================================
    -- PRODUCT CLEANING
    -- ============================================

    TRIM(product_name) AS product_name,

    INITCAP(TRIM(category)) AS category,



    -- ============================================
    -- NUMERIC CLEANING
    -- ============================================

    ABS(price)::BIGINT AS price,

    ABS(quantity)::BIGINT AS quantity,



    -- ============================================
    -- EVENT TYPE CLEANING
    -- ============================================

    LOWER(TRIM(event_type)) AS event_type,



    -- ============================================
    -- TIMESTAMP CLEANING
    -- ============================================

    CASE

        -- invalid timestamps
        WHEN timestamp IS NULL
             OR TRIM(timestamp) = ''
             OR timestamp = 'NaT'
        THEN NULL


        -- ISO timestamp format
        -- example:
        -- 2026-05-13T16:54:05.046763

        WHEN timestamp LIKE '%T%'
        THEN TO_TIMESTAMP(
            SUBSTRING(timestamp, 1, 19),
            'YYYY-MM-DD"T"HH24:MI:SS'
        )


        -- normal timestamp format
        -- example:
        -- 13-05-2026 17:14

        WHEN timestamp LIKE '__-__-____ __:__%'
        THEN TO_TIMESTAMP(
            timestamp,
            'DD-MM-YYYY HH24:MI'
        )

        ELSE NULL

    END AS event_timestamp,



    -- ============================================
    -- PAYMENT METHOD CLEANING
    -- ============================================

    UPPER(TRIM(payment_method)) AS payment_method,



    -- ============================================
    -- CITY STANDARDIZATION
    -- ============================================
CASE

    WHEN city IS NULL
         OR TRIM(city) = ''
         OR LOWER(TRIM(city)) = 'null'
    THEN 'Others'

    WHEN city LIKE '%?%'
    THEN 'Others'

    WHEN LOWER(TRIM(city)) IN (
        'del',
        'delhi',
        'dilli'
    )
    THEN 'Delhi'

    WHEN LOWER(TRIM(city)) IN (
        'mum',
        'mumbai',
        'bombay'
    )
    THEN 'Mumbai'

    WHEN LOWER(TRIM(city)) IN (
        'kol',
        'kolkata',
        'calcutta'
    )
    THEN 'Kolkata'

    WHEN LOWER(TRIM(city)) IN (
        'blr',
        'bangalore',
        'bengaluru'
    )
    THEN 'Bengaluru'



    ELSE INITCAP(TRIM(city))

END AS city,


    -- ============================================
    -- REVENUE CALCULATION
    -- ============================================

    (
        ABS(price)::BIGINT *
        ABS(quantity)::BIGINT
    ) AS revenue


FROM raw_data



-- ============================================
-- REMOVE INVALID RECORDS
-- ============================================

WHERE
    order_id IS NOT NULL
    AND product_id IS NOT NULL
    AND product_name IS NOT NULL
    AND category IS NOT NULL
    AND event_type IS NOT NULL
    AND price IS NOT NULL
    AND quantity IS NOT NULL
    AND price > 0
    AND quantity > 0;



-- ============================================
-- VALIDATION QUERIES
-- ============================================

-- total records
SELECT COUNT(*) FROM cleaned_data;

SELECT * FROM cleaned_data ;



 



-- city distribution
SELECT
    city,
    COUNT(*) AS total_orders
FROM cleaned_data
GROUP BY city
ORDER BY total_orders DESC;



-- payment method distribution
SELECT
    payment_method,
    COUNT(*) AS total_orders
FROM cleaned_data
GROUP BY payment_method
ORDER BY total_orders DESC;

--  True duplicates with timestamp
SELECT 
    order_id,
    product_id,
    price,
    quantity,
    event_timestamp,
    COUNT(*) as count
FROM cleaned_data
GROUP BY order_id, product_id, price, quantity, event_timestamp
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10;

-- event type distribution
SELECT
    event_type,
    COUNT(*) AS total_events
FROM cleaned_data
GROUP BY event_type
ORDER BY total_events DESC;



-- revenue statistics
SELECT
    MIN(revenue) AS min_revenue,
    MAX(revenue) AS max_revenue,
    AVG(revenue) AS avg_revenue
FROM cleaned_data;



-- timestamp range
SELECT
    MIN(event_timestamp) AS oldest_timestamp,
    MAX(event_timestamp) AS latest_timestamp
FROM cleaned_data;



-- preview cleaned data
SELECT *
FROM cleaned_data
LIMIT 20;