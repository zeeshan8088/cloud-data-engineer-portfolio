
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        age_bucket as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_customers`
    group by age_bucket

)

select *
from all_values
where value_field not in (
    '18-25','26-35','36-45','46-55','56+','unknown'
)



  
  
      
    ) dbt_internal_test