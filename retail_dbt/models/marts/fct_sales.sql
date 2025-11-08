{{ config(materialized='table') }}

SELECT
    order_id,
    order_date,
    ship_date,
    customer_name,
    segment,
    country,
    city,
    state,
    region,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    -- calculated fields
    profit / NULLIF(sales, 0)       AS profit_margin,
    sales - profit                  AS cost
FROM {{ ref('stg_orders') }}
