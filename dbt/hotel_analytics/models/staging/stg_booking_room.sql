{{ config(materialized='table') }}

select
  booking_room_id,
  booking_id,
  room_id,
  assigned_at,
  created_at
from {{ source('raw', 'BOOKING_ROOM_RAW') }}
