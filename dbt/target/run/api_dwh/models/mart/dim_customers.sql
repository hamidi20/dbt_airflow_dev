
  
    

  create  table "warehouse"."mart"."dim_customers__dbt_tmp"
  
  
    as
  
  (
    SELECT customer_id,
    TRIM(full_name) AS customer_name,
    LOWER(email) AS email,
    phone,
    created_at,
    updated_at
FROM staging.ods_customers
WHERE is_deleted = FALSE
  );
  