
  
    
    

    create  table
      "my_database"."main"."fct_sales__dbt_tmp"
  
    as (
      

with calls as (
    select * from "my_database"."main"."mart_calls"
),

rep_stats as (
    select
        sales_rep_name,
        count(call_id) as total_calls_made,
        count(distinct lead_id) as unique_leads_contacted
    from calls
    group by sales_rep_name
),

ranked_sales_reps as (
    select
        sales_rep_name,
        total_calls_made,
        unique_leads_contacted,
        rank() over (order by total_calls_made desc) as performance_rank
    from rep_stats
)

select * from ranked_sales_reps
    );
  
  