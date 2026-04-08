
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_clickstream`
where event_id is null



  
  
      
    ) dbt_internal_test