{{ config(materialized='table') }}

-- Build a date spine from min(order_date) to max(order_date) with a small buffer
with bounds as (
  select
    dateadd(day, -7, min(order_date)) as start_date,
    dateadd(day,  7, max(order_date)) as end_date
  from {{ ref('stg_orders') }}
),
seq as (
  -- generate “enough” rows (bump rowcount if your range is larger)
  select seq4() as n
  from table(generator(rowcount => 20000))
),
gen as (
  select
    dateadd(day, s.n, b.start_date) as date_day
  from seq s
  cross join bounds b
  where dateadd(day, s.n, b.start_date) <= b.end_date
)
select
  date_day                                   as date,
  year(date_day)                              as year,
  to_char(date_day, 'YYYY-MM')                as year_month_label,
  date_trunc('month', date_day)               as month_start,
  month(date_day)                             as month_num,
  to_char(date_day, 'Mon')                    as month_name,
  quarter(date_day)                           as quarter_num,
  date_trunc('quarter', date_day)             as quarter_start,
  day(date_day)                               as day_of_month,
  dayofweek(date_day)                         as day_of_week_num,
  to_char(date_day, 'Dy')                     as day_of_week_name
from gen
