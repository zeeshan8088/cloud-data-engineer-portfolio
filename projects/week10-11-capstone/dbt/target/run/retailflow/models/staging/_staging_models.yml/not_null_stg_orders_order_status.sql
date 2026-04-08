
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_status
from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_orders`
where order_status is null



  
  
      
    ) dbt_internal_test