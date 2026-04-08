
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select category
from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_products`
where category is null



  
  
      
    ) dbt_internal_test