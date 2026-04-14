

with source as (
    select * from read_csv_auto('../include/applicant.csv')
)

select * from source