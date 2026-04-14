
  
    
    

    create  table
      "my_database"."main"."raw_leads__dbt_tmp"
  
    as (
      

with source as (
    select * from read_json_auto('../data/leads_data/*.json')
)

select * from source
    );
  
  