{{ config(tags=['daily']) }}

with raw_data as (
    select * from {{ ref('raw_applicant') }}
),

renamed as (
    select
        cast(key as integer) as applicant_id,
        applicant_name,
        role as applicant_role,
        cast(date as date) as application_date,
        phone as applicant_phone,
        email as applicant_email,
        status as application_status,
        cast(program_key as integer) as program_id,
        cast(coordinator_key as integer) as coordinator_id,
        cast(lead_key as integer) as lead_id
    from raw_data
)

select * from renamed