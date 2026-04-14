{{ config(tags=['daily']) }}

with funnel_data as (
    select * from {{ ref('mart_leads_to_applicants') }}
),

funnel_aggregated as (
    select
        platform_name,
        count(lead_id) as total_leads,
        sum(case when is_converted = true then 1 else 0 end) as total_applicants
    from funnel_data
    group by platform_name
)

select
    platform_name,
    total_leads,
    total_applicants,
    case
        when total_leads > 0 then round((total_applicants * 100.0) / total_leads, 2)
        else 0
    end as conversion_rate_percent
from funnel_aggregated