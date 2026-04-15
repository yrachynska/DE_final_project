{{ config(materialized='table', tags=['daily']) }}

with source as (
    select * from read_csv_auto('s3://marketing/advertising_companies.csv')
)

select * from source