{{ config(materialized='table') }}

select
  booking_id,
  customer_id,
  hotel_id,
  channel_id,
  booking_status,
  booking_channel,
  checkin_date,
  checkout_date,
  created_at,
  updated_at
from {{ source('raw', 'BOOKING_RAW') }}
