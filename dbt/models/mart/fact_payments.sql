SELECT p.payment_id,
    p.order_id,
    o.customer_id,
    TRIM(c.full_name) AS customer_name,
    p.payment_method,
    p.amount,
    p.payment_date,
    DATE(p.payment_date) AS payment_day
FROM staging.ods_payments p
    LEFT JOIN staging.ods_orders o ON p.order_id::VARCHAR = o.order_id
    LEFT JOIN staging.ods_customers c ON o.customer_id::VARCHAR = c.customer_id
WHERE c.is_deleted = FALSE