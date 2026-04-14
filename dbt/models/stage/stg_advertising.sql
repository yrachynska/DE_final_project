{{ config(tags=['daily']) }}

with raw_data as (
    select * from {{ ref('raw_advertising') }}
),

renamed as (
    select
        cast(key as integer) as campaign_id,
        name as campaign_name,
        resource as platform_name,
        cast(date_start as date) as start_date,
        cast(date_end as date) as end_date,
        cast(cost as double) as total_cost
    from raw_data
)

select * from renamed