{{ config(materialized='table') }}

with base as (
  select
    order_id,
    order_date::date  as order_date,
    ship_date::date   as ship_date,
    datediff('day', order_date, ship_date) as days_to_ship,
    cast(sales  as float) as sales,
    cast(profit as float) as profit,
    region,
    category,
    sub_category
  from {{ ref('stg_orders') }}
  where order_date is not null and ship_date is not null
)
select
  region,
  category,
  sub_category,
  count(distinct order_id)                                       as orders,
  cast(avg(days_to_ship) as float)                               as avg_days_to_ship,
  percentile_cont(0.5) within group (order by days_to_ship)      as p50_days_to_ship,
  percentile_cont(0.9) within group (order by days_to_ship)      as p90_days_to_ship,
  cast(sum(sales)  as float)                                     as total_sales,
  cast(sum(profit) as float)                                     as total_profit
from base
group by 1,2,3
