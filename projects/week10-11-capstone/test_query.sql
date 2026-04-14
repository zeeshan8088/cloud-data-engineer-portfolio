with mart_total as (
    select round(sum(total_revenue), 2) as mart_revenue
    from `retailflow_gold.mart_sales_daily`
),
staging_total as (
    select round(sum(total_amount), 2) as staging_revenue
    from `retailflow_silver.stg_orders`
    where order_status != 'cancelled'
)
select * from mart_total cross join staging_total
