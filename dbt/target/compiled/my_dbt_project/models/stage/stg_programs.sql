

with source as (
    select * from "my_database"."main"."programs"
),

renamed as (
    select
        cast(key as integer) as program_id,
        name as program_name,
        duration as program_duration,
        level as program_level,
        cast(cost as double) as tuition_fees
    from source
)

select * from renamed