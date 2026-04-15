
    
    

select
    key as unique_field,
    count(*) as n_records

from "my_database"."main"."raw_advertising_companies"
where key is not null
group by key
having count(*) > 1


