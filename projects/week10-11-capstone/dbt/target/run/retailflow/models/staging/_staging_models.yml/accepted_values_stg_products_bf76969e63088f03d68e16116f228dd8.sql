
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        category as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_silver`.`stg_products`
    group by category

)

select *
from all_values
where value_field not in (
    'electronics','clothing','home_kitchen','books','sports_fitness','beauty_personal_care','toys_games'
)



  
  
      
    ) dbt_internal_test