{{ config(materialized='table') }}

select
  room_type_id,
  NAME room_type_name,
  MAX_OCCUPANCY capacity,
  base_rate
from {{ ref('stg_room_type') }}
