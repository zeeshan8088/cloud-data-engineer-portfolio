
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_orders`
    group by payment_method

)

select *
from all_values
where value_field not in (
    'credit_card','debit_card','upi','net_banking','cod','wallet'
)



  
  
      
    ) dbt_internal_test