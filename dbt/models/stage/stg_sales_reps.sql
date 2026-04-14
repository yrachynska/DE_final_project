{{ config(tags=['daily']) }}

with source as (
    select * from {{ ref('employees_sales') }}
),

renamed as (
    select
        cast(key as integer) as sales_rep_id,
        replace(name_surname, 'пані ', '') as full_name,
        cast(head_key as integer) as manager_id,
        cast(date_start as date) as hire_date,
        cast(date_end as date) as termination_date,
        email as work_email,
        phone as work_phone,
        cast(salary as double) as base_salary
    from source
)

select * from renamed