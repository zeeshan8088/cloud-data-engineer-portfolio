
    
    

with all_values as (

    select
        funnel_step_number as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_clickstream`
    group by funnel_step_number

)

select *
from all_values
where value_field not in (
    1,2,3,4,5
)


