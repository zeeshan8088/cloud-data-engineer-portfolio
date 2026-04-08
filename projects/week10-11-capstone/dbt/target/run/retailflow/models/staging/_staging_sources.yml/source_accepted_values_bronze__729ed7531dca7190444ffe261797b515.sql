
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_customers`
    group by gender

)

select *
from all_values
where value_field not in (
    'Male','Female','Non-Binary'
)



  
  
      
    ) dbt_internal_test