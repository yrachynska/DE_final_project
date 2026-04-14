

with raw_data as (
    select * from "my_database"."main"."raw_calls"
),

renamed as (
    select
        cast(key as integer) as call_id,
        cast(sales_key as integer) as sales_rep_id,
        cast(lead_key as integer) as lead_id,
        cast(time as timestamp) as call_timestamp,
        phone as contact_phone,
        cast(duration as time) as call_duration
    from raw_data
)

select * from renamed