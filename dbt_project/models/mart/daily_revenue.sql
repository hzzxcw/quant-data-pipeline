SELECT
    order_date,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(amount) AS avg_order_value
FROM {{ ref('stg_orders') }}
GROUP BY order_date
ORDER BY order_date
