{{ config(tags=['daily']) }}

with source as (
    select * from {{ source('raw_data', 'raw_advertising_companies') }}
)

select * from source