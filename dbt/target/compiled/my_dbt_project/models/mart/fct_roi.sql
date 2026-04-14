

with leads_with_ads as (
    select * from "my_database"."main"."mart_leads"
),

campaign_metrics as (
    select
        campaign_name,
        platform_name,
        max(total_cost) as total_budget,
        count(lead_id) as total_leads_generated
    from leads_with_ads
    group by campaign_name, platform_name
)

select
    campaign_name,
    platform_name,
    total_budget,
    total_leads_generated,
    case
        when total_leads_generated > 0 then round(total_budget / total_leads_generated, 2)
        else 0
    end as cost_per_lead
from campaign_metrics