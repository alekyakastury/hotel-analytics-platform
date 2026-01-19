{{ config(materialized='table') }}

with hotel as (
  select
    hotel_id,
    name as hotel_name,
    address_id
  from {{ ref('stg_hotel') }}
),

address as (
  select
    address_id,
    city,
    state,
    country
  from {{ ref('stg_address') }}
)

select
  h.hotel_id,
  h.hotel_name,
  a.city,
  a.state,
  a.country
from hotel h
left join address a
  on h.address_id = a.address_id
