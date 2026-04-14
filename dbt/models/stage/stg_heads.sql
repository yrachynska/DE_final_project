{{ config(tags=['daily']) }}

with source as (
    select * from {{ ref('heads') }}
),

renamed as (
    select
        cast(key as integer) as head_id,
        name_surname as head_name,
        cast(date_start as date) as hire_date,
        cast(date_end as date) as termination_date,
        email as head_email,
        phone as head_phone,
        cast(salary as double) as base_salary
    from source
)

select * from renamed