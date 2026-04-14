

with calls as (
    select * from "my_database"."main"."stg_calls"
),

sales as (
    select * from "my_database"."main"."stg_sales_reps"
),

joined as (
    select
        calls.call_id,
        calls.lead_id,
        calls.call_timestamp,
        calls.call_duration,
        sales.full_name as sales_rep_name
    from calls
    left join sales on calls.sales_rep_id = sales.sales_rep_id
)

select * from joined