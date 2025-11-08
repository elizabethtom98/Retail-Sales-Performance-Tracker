{{ config(materialized='table') }}

select
  md5(coalesce(customer_name,'') || '|' || coalesce(segment,'')) as customer_key,
  customer_name,
  segment
from {{ ref('stg_orders') }}
group by 1,2,3

