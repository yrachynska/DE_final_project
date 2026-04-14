
  
    
    

    create  table
      "my_database"."main"."raw_applicant__dbt_tmp"
  
    as (
      

with source as (
    select * from read_csv_auto('../include/applicant.csv')
)

select * from source
    );
  
  