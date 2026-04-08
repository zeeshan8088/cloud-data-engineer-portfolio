
    
    

with dbt_test__target as (

  select customer_version_sk as unique_field
  from `intricate-ward-459513-e1`.`retailflow_silver`.`dim_customers_scd2`
  where customer_version_sk is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


