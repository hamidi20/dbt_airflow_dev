
  
    

  create  table "warehouse"."mart"."fact_sales_summary__dbt_tmp"
  
  
    as
  
  (
    SELECT DATE(o.order_date) AS sales_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    SUM(o.qty) AS total_qty,
    SUM(o.qty * o.price) AS gross_sales,
    AVG(o.qty * o.price) AS avg_order_value
FROM staging.ods_orders o
GROUP BY DATE(o.order_date)
  );
  