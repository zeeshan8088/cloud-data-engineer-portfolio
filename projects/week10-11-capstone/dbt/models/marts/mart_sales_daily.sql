-- ============================================================
-- RetailFlow — mart_sales_daily (Gold Layer)
-- ============================================================
-- Business-ready daily sales aggregation table.
-- Designed for Looker Studio "Revenue Overview" dashboard page.
--
-- Grain: one row per calendar date
-- Sources: stg_orders (revenue), stg_products (category detail)
--
-- Key metrics:
--   total_revenue, order_count, avg_order_value,
--   unique_customers, units_sold, top_category,
--   cancelled_orders, cancellation_rate
--
-- Partitioned by: order_date (day)
-- Clustered by:   top_category
-- Materialized as: TABLE (see dbt_project.yml → marts)
-- ============================================================

{{
    config(
        materialized='table',
        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by="top_category",
        tags=['gold', 'mart', 'sales']
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

-- Join orders → products to get category for each line
order_with_category as (

    select
        o.order_date,
        o.order_id,
        o.customer_id,
        o.total_amount,
        o.quantity,
        o.order_status,
        o.payment_method,
        coalesce(p.category, o.product_category) as category

    from orders o
    left join products p
        on o.product_id = p.product_id

),

-- Per-date category ranking (to find the top-selling category)
category_revenue as (

    select
        order_date,
        category,
        sum(total_amount) as cat_revenue,
        row_number() over (
            partition by order_date
            order by sum(total_amount) desc
        ) as cat_rank

    from order_with_category
    where order_status != 'cancelled'
    group by order_date, category

),

top_cat as (

    select
        order_date,
        category as top_category

    from category_revenue
    where cat_rank = 1

),

-- Main daily aggregation
daily_agg as (

    select
        order_date,

        -- ── Revenue metrics ─────────────────────────────────────
        round(sum(case when order_status != 'cancelled'
                       then total_amount else 0 end), 2)
            as total_revenue,

        count(distinct order_id)
            as order_count,

        coalesce(round(safe_divide(
            sum(case when order_status != 'cancelled'
                     then total_amount else 0 end),
            countif(order_status != 'cancelled')
        ), 2), 0)
            as avg_order_value,

        -- ── Volume metrics ──────────────────────────────────────
        count(distinct customer_id)
            as unique_customers,

        sum(case when order_status != 'cancelled'
                 then quantity else 0 end)
            as units_sold,

        -- ── Cancellation tracking ───────────────────────────────
        countif(order_status = 'cancelled')
            as cancelled_orders,

        round(safe_divide(
            countif(order_status = 'cancelled'),
            count(distinct order_id)
        ) * 100, 2)
            as cancellation_rate,

        -- ── Payment mix ─────────────────────────────────────────
        countif(payment_method = 'upi')         as upi_orders,
        countif(payment_method = 'credit_card')  as credit_card_orders,
        countif(payment_method = 'cod')          as cod_orders

    from order_with_category
    group by order_date

)

select
    d.*,
    t.top_category

from daily_agg d
left join top_cat t
    on d.order_date = t.order_date
