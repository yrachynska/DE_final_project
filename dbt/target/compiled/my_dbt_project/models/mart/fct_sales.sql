

with calls as (
    select * from "my_database"."main"."mart_calls"
),

leads_to_apps as (
    select * from "my_database"."main"."mart_leads_to_applicants"
),

rep_stats as (
    select
        c.sales_rep_name,
        count(c.call_id) as total_calls_made,
        count(distinct c.lead_id) as unique_leads_contacted,
        count(distinct la.applicant_id) as total_applicants_generated
    from calls c
    left join leads_to_apps la on c.lead_id = la.lead_id
    group by c.sales_rep_name
),

ranked_sales_reps as (
    select
        sales_rep_name,
        total_calls_made,
        unique_leads_contacted,
        total_applicants_generated,
        case
            when unique_leads_contacted > 0
            then round((total_applicants_generated * 100.0) / unique_leads_contacted, 2)
            else 0
        end as conversion_rate_percent,
        rank() over (order by total_applicants_generated desc, (total_applicants_generated * 1.0 / nullif(unique_leads_contacted, 0)) desc) as calls_result_rank
    from rep_stats
)

select * from ranked_sales_reps