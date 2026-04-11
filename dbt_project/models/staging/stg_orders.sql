-- 清洗订单数据
-- 移除取消的订单，标准化字段

SELECT
    order_id,
    customer_id,
    CAST(amount AS DECIMAL(10, 2)) AS amount,
    status,
    CAST(created_at AS TIMESTAMP) AS created_at,
    DATE(created_at) AS order_date
FROM {{ source('raw', 'orders') }}
WHERE status != 'cancelled'
