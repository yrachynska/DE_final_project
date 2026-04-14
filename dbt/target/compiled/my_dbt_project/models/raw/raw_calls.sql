

with source as (
    select * from read_json_auto('../data/calls_data/*.json')
)

select * from source