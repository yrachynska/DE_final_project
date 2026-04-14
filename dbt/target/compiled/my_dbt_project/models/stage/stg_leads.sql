

with raw_data as (
    select * from "my_database"."main"."raw_leads"
),

renamed as (
    select
        cast(key as integer) as lead_id,
        lead_name,
        cast(advertising_company_key as integer) as campaign_id,
        cast(date as date) as lead_date,
        phone as lead_phone,
        email as lead_email,
        status as lead_status
    from raw_data
)

select * from renamed