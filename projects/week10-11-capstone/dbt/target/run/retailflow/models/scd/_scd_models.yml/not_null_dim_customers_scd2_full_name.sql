
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select full_name
from `intricate-ward-459513-e1`.`retailflow_silver`.`dim_customers_scd2`
where full_name is null



  
  
      
    ) dbt_internal_test