{{ config(materialized='table') }}

with spine as (
  select dateadd(day, seq4(), to_date('2023-01-01')) as date_day
  from table(generator(rowcount => 4000))
)
select
  date_day,
  year(date_day) as year,
  month(date_day) as month,
  monthname(date_day) as month_name,
  quarter(date_day) as quarter,
  weekofyear(date_day) as week_of_year,
  dayofweek(date_day) as day_of_week,
  dayname(date_day) as day_name,
  case when dayofweek(date_day) in (0,6) then true else false end as is_weekend
from spine
where date_day <= current_date()
