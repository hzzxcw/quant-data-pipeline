{{
    config(
        materialized='table'
    )
}}

WITH lagged AS (
    SELECT
        code,
        date,
        close,
        LAG(close) OVER (PARTITION BY code ORDER BY date) AS prev_close
    FROM {{ ref('stg_stock_quotes') }}
)
SELECT
    code,
    date,
    close,
    prev_close,
    ROUND((close - prev_close) / prev_close * 100, 4) AS daily_return_pct
FROM lagged
WHERE prev_close IS NOT NULL
