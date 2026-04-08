
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_clickstream`
    group by event_type

)

select *
from all_values
where value_field not in (
    'page_view','product_view','add_to_cart','checkout_started','purchase'
)



  
  
      
    ) dbt_internal_test