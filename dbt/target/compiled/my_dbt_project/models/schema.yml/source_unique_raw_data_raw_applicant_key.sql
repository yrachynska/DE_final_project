
    
    

select
    key as unique_field,
    count(*) as n_records

from "my_database"."main"."raw_applicant"
where key is not null
group by key
having count(*) > 1


