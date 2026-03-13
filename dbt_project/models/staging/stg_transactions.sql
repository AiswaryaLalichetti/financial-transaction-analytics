WITH source AS (
    SELECT * FROM {{ source('raw', 'TRANSACTIONS') }}
),

cleaned AS (
    SELECT
        TRANSACTION_ID,
        CUSTOMER_ID,
        MERCHANT_ID,
        TRANSACTION_DATE,
        AMOUNT,
        CURRENCY,
        TRANSACTION_TYPE,
        CHANNEL,
        STATUS,
        IS_FRAUD,
        CASE
            WHEN AMOUNT < 100 THEN 'Low'
            WHEN AMOUNT < 1000 THEN 'Medium'
            WHEN AMOUNT < 3000 THEN 'High'
            ELSE 'Very High'
        END AS AMOUNT_CATEGORY,
        YEAR(TRANSACTION_DATE) AS YEAR,
        MONTH(TRANSACTION_DATE) AS MONTH,
        DAY(TRANSACTION_DATE) AS DAY,
        DAYOFWEEK(TRANSACTION_DATE) AS DAY_OF_WEEK,
        CASE
            WHEN DAYOFWEEK(TRANSACTION_DATE) IN (0,6) THEN 1
            ELSE 0
        END AS IS_WEEKEND
    FROM source
    WHERE AMOUNT > 0
    AND STATUS IN ('Completed', 'Pending', 'Failed')
)

SELECT * FROM cleaned