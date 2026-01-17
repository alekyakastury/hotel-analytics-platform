{{ config(materialized='table') }}

select
  hotel_id,
  name,
  brand,
  address_id,
  timezone,
  created_at,
  updated_at
from {{ source('raw', 'HOTEL_RAW') }}
