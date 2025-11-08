{{ config(materialized='table') }}

select
  md5(coalesce(category,'') || '|' || coalesce(sub_category,'') || '|' || coalesce(product_name,'')) as product_key,
  category,
  sub_category,
  product_name
from {{ ref('stg_orders') }}
group by 1,2,3,4

