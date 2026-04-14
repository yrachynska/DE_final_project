{{ config(tags=['daily']) }}

with source as (
    select * from {{ source('raw_data', 'raw_applicant') }}
)

select * from source