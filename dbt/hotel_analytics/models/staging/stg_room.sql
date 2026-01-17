{{ config(materialized='table') }}

select
  room_id,
  hotel_id,
  room_type_id,
  room_number,
  floor,
  status,
  created_at,
  updated_at
from {{ source('raw', 'ROOM_RAW') }}
