
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_method
from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_orders`
where payment_method is null



  
  
      
    ) dbt_internal_test