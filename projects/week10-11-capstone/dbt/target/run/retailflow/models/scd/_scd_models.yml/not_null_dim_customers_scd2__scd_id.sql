
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _scd_id
from `intricate-ward-459513-e1`.`retailflow_silver`.`dim_customers_scd2`
where _scd_id is null



  
  
      
    ) dbt_internal_test