{{ config(materialized='table') }}

select
  booking_id,
  min(hotel_id) as hotel_id,
  min(booking_status) as booking_status,
  min(checkin_date) as checkin_date,
  min(checkout_date) as checkout_date,
  min(nights) as nights,
  sum(room_amount) as total_amount
from {{ ref('fct_booking_room') }}
group by booking_id
