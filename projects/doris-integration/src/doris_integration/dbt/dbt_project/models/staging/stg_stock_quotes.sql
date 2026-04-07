{{
    config(
        materialized='view'
    )
}}

SELECT
    code,
    date,
    open,
    high,
    low,
    close,
    volume,
    amount
FROM {{ source('raw', 'stock_quotes') }}
WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
