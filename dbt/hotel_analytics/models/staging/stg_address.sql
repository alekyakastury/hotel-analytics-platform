{{ config(materialized='table') }}

select
    ADDRESS_ID,
    STREET,
    CITY,
    STATE,
    COUNTRY,
    POSTAL_CODE,
    CREATED_AT
from {{ source('raw', 'ADDRESS_RAW') }}
