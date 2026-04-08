
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from `intricate-ward-459513-e1`.`retailflow_silver`.`dim_customers_scd2`
where customer_id is null



  
  
      
    ) dbt_internal_test