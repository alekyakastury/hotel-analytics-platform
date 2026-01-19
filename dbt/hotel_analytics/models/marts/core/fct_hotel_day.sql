{{ config(materialized='table') }}

with bookings as (
  select
    booking_id,
    hotel_id,
    booking_ts::date as date_day,
    booking_status,
    total_amount,
    datediff('day', checkin_date, checkout_date) as nights
  from {{ ref('fct_booking') }}
),
daily as (
  select
    hotel_id,
    date_day,
    count(*) as bookings,
    count_if(lower(booking_status) like '%cancel%') as cancellations,
    sum(total_amount) as gross_revenue,
    avg(total_amount) as avg_booking_value,
    avg(nights) as avg_length_of_stay
  from bookings
  group by 1,2
)
select
  d.date_day,
  h.hotel_id,
  h.hotel_name,
  h.city,
  h.country,
  coalesce(daily.bookings, 0) as bookings,
  coalesce(daily.cancellations, 0) as cancellations,
  coalesce(daily.gross_revenue, 0) as gross_revenue,
  daily.avg_booking_value,
  daily.avg_length_of_stay
from {{ ref('date_dim') }} d
cross join {{ ref('dim_hotel') }} h
left join daily
  on daily.date_day = d.date_day
 and daily.hotel_id = h.hotel_id
where d.date_day <= current_date()
