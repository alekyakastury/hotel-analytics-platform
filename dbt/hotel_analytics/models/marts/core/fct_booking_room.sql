{{ config(materialized='table') }}

with booking_room as (
  select
    booking_id,
    room_id
  from {{ ref('stg_booking_room') }}
),

bookings as (
  select
    booking_id,
    hotel_id,
    booking_status,
    checkin_date,
    checkout_date,
    datediff('day', checkin_date, checkout_date) as nights
  from {{ ref('stg_bookings') }}
),

rooms as (
  select
    room_id,
    room_type_id
  from {{ ref('stg_room') }}
),

room_type as (
  select
    room_type_id,
    base_rate
  from {{ ref('stg_room_type') }}
)

select
  br.booking_id,
  br.room_id,
  b.hotel_id,
  b.booking_status,
  b.checkin_date,
  b.checkout_date,
  b.nights,
  r.room_type_id,
  rt.base_rate,
  (rt.base_rate * b.nights) as room_amount
from booking_room br
join bookings b
  on br.booking_id = b.booking_id
left join rooms r
  on br.room_id = r.room_id
left join room_type rt
  on r.room_type_id = rt.room_type_id
