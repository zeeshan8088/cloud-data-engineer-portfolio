
    
    

with all_values as (

    select
        stock_status as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_products`
    group by stock_status

)

select *
from all_values
where value_field not in (
    'out_of_stock','low_stock','in_stock','overstocked'
)


