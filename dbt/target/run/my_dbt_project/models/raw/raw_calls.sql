
  
    
    

    create  table
      "my_database"."main"."raw_calls__dbt_tmp"
  
    as (
      

with source as (
    select * from read_json_auto('../data/calls_data/*.json')
)

select * from source
    );
  
  