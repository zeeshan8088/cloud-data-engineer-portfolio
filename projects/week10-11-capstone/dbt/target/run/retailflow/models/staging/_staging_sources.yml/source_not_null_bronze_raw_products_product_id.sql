
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_id
from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_products`
where product_id is null



  
  
      
    ) dbt_internal_test