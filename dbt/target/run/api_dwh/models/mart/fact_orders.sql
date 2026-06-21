
  
    

  create  table "warehouse"."mart"."fact_orders__dbt_tmp"
  
  
    as
  
  (
    SELECT o.order_id,
    o.customer_id,
    TRIM(c.full_name) AS customer_name,
    LOWER(c.email) AS customer_email,
    o.product_name,
    o.qty,
    o.price,
    (o.qty * o.price) AS total_amount,
    o.order_date,
    DATE(o.order_date) AS order_day,
    EXTRACT(
        YEAR
        FROM o.order_date
    ) AS order_year,
    EXTRACT(
        MONTH
        FROM o.order_date
    ) AS order_month
FROM staging.ods_orders o
    LEFT JOIN staging.ods_customers c ON o.customer_id::VARCHAR = c.customer_id
WHERE c.is_deleted = FALSE
  );
  