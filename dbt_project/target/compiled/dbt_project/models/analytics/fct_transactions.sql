WITH transactions AS (
    SELECT * FROM FINTECH_DB.ANALYTICS_RAW.stg_transactions
),

customers AS (
    SELECT * FROM FINTECH_DB.RAW.CUSTOMERS
),

merchants AS (
    SELECT * FROM FINTECH_DB.RAW.MERCHANTS
),

final AS (
    SELECT
        t.TRANSACTION_ID,
        t.CUSTOMER_ID,
        t.MERCHANT_ID,
        t.TRANSACTION_DATE,
        t.AMOUNT,
        t.AMOUNT_CATEGORY,
        t.CURRENCY,
        t.TRANSACTION_TYPE,
        t.CHANNEL,
        t.STATUS,
        t.IS_FRAUD,
        t.IS_WEEKEND,
        t.YEAR,
        t.MONTH,
        CASE
            WHEN t.IS_FRAUD = 1 AND t.AMOUNT > 3000 THEN 0.95
            WHEN t.IS_FRAUD = 1 THEN 0.75
            WHEN t.AMOUNT > 3000 AND t.IS_WEEKEND = 1 THEN 0.45
            WHEN t.AMOUNT > 3000 THEN 0.30
            ELSE 0.05
        END AS FRAUD_RISK_SCORE,
        c.NAME AS CUSTOMER_NAME,
        c.ACCOUNT_TYPE,
        c.CREDIT_SCORE,
        c.STATE AS CUSTOMER_STATE,
        m.MERCHANT_NAME,
        m.CATEGORY AS MERCHANT_CATEGORY,
        m.CITY AS MERCHANT_CITY
    FROM transactions t
    LEFT JOIN customers c ON t.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN merchants m ON t.MERCHANT_ID = m.MERCHANT_ID
)

SELECT * FROM final