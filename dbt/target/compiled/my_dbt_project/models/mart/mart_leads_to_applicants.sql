

with enriched_leads as (
    select * from "my_database"."main"."mart_leads"
),

applicants as (
    select * from "my_database"."main"."stg_applicant"
),

joined as (
    select
        l.lead_id,
        l.lead_date,
        l.campaign_name,
        l.platform_name,
        a.applicant_id,
        a.application_date,
        a.application_status,
        case
            when a.applicant_id is not null then true
            else false
        end as is_converted
    from enriched_leads l
    left join applicants a on l.lead_id = a.lead_id
)

select * from joined