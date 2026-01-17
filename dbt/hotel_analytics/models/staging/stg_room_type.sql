{{ config(materialized='table') }}

select
  room_type_id,
  name,
  max_occupancy,
  base_rate,
  created_at,
  updated_at
from {{ source('raw', 'ROOM_TYPE_RAW') }}
