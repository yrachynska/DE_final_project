

with source as (
    select * from "my_database"."main"."employees_coordinators"
),

renamed as (
    select
        cast(key as integer) as coordinator_id,
        name_surname as full_name,
        cast(head_key as integer) as head_id,
        cast(date_start as date) as hire_date,
        cast(date_end as date) as termination_date,
        email as work_email,
        phone as work_phone
    from source
)

select * from renamed