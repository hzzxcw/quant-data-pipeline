SELECT
    c.customer_id,
    c.name,
    c.email,
    c.region,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_spent,
    AVG(o.amount) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3, 4
