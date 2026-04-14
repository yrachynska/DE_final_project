{{ config(tags=['hourly']) }}

with leads as (
    select * from {{ ref('stg_leads') }}
),

ads as (
    select * from {{ ref('stg_advertising') }}
),

joined as (
    select
        leads.lead_id,
        leads.lead_name,
        leads.lead_date,
        leads.lead_status,
        ads.campaign_name,
        ads.platform_name,
        ads.total_cost
    from leads
    left join ads on leads.campaign_id = ads.campaign_id
)

select * from joined