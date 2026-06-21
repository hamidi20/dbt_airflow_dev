
  create view "warehouse"."mart_staging"."stg_customers__dbt_tmp"
    
    
  as (
    SELECT customer_id,
    TRIM(full_name) AS full_name,
    LOWER(email) AS email,
    phone,
    created_at,
    updated_at,
    is_deleted
FROM master.public.ods_customers
WHERE is_deleted = FALSE
  );