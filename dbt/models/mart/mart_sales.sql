{{ config(tags=['daily']) }}

with sales as (
    select * from {{ ref('stg_sales_reps') }}
),

heads as (
    select * from {{ ref('stg_heads') }}
),

joined as (
    select
        sales.sales_rep_id,
        sales.full_name as sales_rep_name,
        sales.base_salary as rep_salary,
        heads.full_name as manager_name,
        heads.head_email as manager_email
    from sales
    left join heads on sales.manager_id = heads.head_id
)

select * from joined