{{ config(tags=['hourly']) }}

with source as (
    select * from {{ source('raw_data', 'raw_leads') }}
)

select * from source