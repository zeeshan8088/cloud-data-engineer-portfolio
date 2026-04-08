
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_orders`
    group by order_status

)

select *
from all_values
where value_field not in (
    'pending','shipped','delivered','cancelled'
)


