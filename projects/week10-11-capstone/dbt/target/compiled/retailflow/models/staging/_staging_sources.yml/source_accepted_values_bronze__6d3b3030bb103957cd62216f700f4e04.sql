
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_orders`
    group by status

)

select *
from all_values
where value_field not in (
    'pending','shipped','delivered','cancelled'
)


