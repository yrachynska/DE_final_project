
  
    
    

    create  table
      "my_database"."main"."raw_advertising__dbt_tmp"
  
    as (
      

with source as (
    select * from read_csv_auto('s3://advertising/advertising_companies.csv')
)

select * from source
    );
  
  