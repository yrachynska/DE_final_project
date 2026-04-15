{{ config(tags=['daily']) }}

with coordinators as (
    select * from {{ ref('stg_coordinators') }}
),

applicants as (
    select * from {{ ref('stg_applicant') }}
),

workload as (
    select
        c.coordinator_id,
        c.full_name as coordinator_name,
        count(a.applicant_id) as total_applicants_assigned
    from coordinators c
    left join applicants a on c.coordinator_id = a.coordinator_id
    group by c.coordinator_id, c.full_name
)

select
    coordinator_id,
    coordinator_name,
    total_applicants_assigned,
    rank() over (order by total_applicants_assigned desc) as workload_rank
from workload
order by workload_rank