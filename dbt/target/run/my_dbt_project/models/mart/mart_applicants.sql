
  
    
    

    create  table
      "my_database"."main"."mart_applicants__dbt_tmp"
  
    as (
      

with applicants as (
    select * from "my_database"."main"."stg_applicant"
),

programs as (
    select * from "my_database"."main"."stg_programs"
),

coordinators as (
    select * from "my_database"."main"."stg_coordinators"
),

joined as (
    select
        a.applicant_id,
        a.applicant_name,
        a.application_date,
        a.application_status,
        p.program_name,
        p.program_level,
        p.tuition_fees,
        c.full_name as coordinator_name
    from applicants a
    left join programs p on a.program_id = p.program_id
    left join coordinators c on a.coordinator_id = c.coordinator_id
)

select * from joined
    );
  
  