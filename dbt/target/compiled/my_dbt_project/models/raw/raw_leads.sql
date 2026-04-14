

with source as (
    select * from read_json_auto('../data/leads_data/*.json')
)

select * from source