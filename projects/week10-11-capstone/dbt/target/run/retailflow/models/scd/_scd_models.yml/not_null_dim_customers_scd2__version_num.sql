
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _version_num
from `intricate-ward-459513-e1`.`retailflow_silver`.`dim_customers_scd2`
where _version_num is null



  
  
      
    ) dbt_internal_test