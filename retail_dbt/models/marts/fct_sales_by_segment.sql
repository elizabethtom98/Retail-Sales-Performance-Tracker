{{ config(materialized='table') }}

select
  segment,
  count(distinct order_id)                                        as orders,
  cast(sum(sales)  as float)                                      as total_sales,
  cast(sum(profit) as float)                                      as total_profit,
  case when sum(sales) = 0 then null
       else sum(profit) / nullif(sum(sales), 0)
  end                                                             as profit_margin,
  cast(sum(sales) as float) / nullif(count(distinct order_id), 0) as avg_order_value
from {{ ref('stg_orders') }}
group by 1
