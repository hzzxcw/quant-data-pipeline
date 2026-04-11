SELECT
    customer_id,
    name,
    email,
    region
FROM {{ source('raw', 'customers') }}
