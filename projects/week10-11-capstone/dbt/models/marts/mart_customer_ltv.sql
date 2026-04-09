-- ============================================================
-- RetailFlow — mart_customer_ltv (Gold Layer)
-- ============================================================
-- Customer Lifetime Value (LTV) analysis table.
-- Designed for Looker Studio "Customer Analytics" dashboard page.
--
-- Grain: one row per customer (current version from SCD2)
-- Sources: stg_orders, stg_customers, dim_customers_scd2
--
-- Key metrics:
--   total_spend, order_count, avg_order_value,
--   days_since_last_order, ltv_segment, churn_risk
--
-- Materialized as: TABLE (refreshed daily)
-- ============================================================

{{
    config(
        materialized='table',
        tags=['gold', 'mart', 'customer']
    )
}}

with current_customers as (

    -- Use the CURRENT version of each customer from SCD2
    select
        customer_id,
        full_name,
        first_name,
        last_name,
        email,
        city,
        state,
        country,
        age,
        age_bucket,
        gender,
        registration_date,
        is_active

    from {{ ref('dim_customers_scd2') }}
    where _is_current = true

),

orders as (

    select * from {{ ref('stg_orders') }}
    where order_status != 'cancelled'

),

-- Per-customer order aggregation
customer_orders as (

    select
        customer_id,

        -- ── Spend metrics ───────────────────────────────────────
        round(sum(total_amount), 2)         as total_spend,
        count(distinct order_id)            as order_count,
        round(avg(total_amount), 2)         as avg_order_value,
        round(sum(quantity), 0)             as total_units_purchased,

        -- ── Temporal metrics ────────────────────────────────────
        min(order_date)                     as first_order_date,
        max(order_date)                     as last_order_date,
        date_diff(current_date(), max(order_date), DAY)
                                            as days_since_last_order,
        date_diff(max(order_date), min(order_date), DAY)
                                            as customer_tenure_days,

        -- ── Frequency ───────────────────────────────────────────
        round(safe_divide(
            count(distinct order_id),
            greatest(date_diff(max(order_date), min(order_date), DAY), 1)
        ) * 30, 2)
            as orders_per_month,

        -- ── Payment preferences ─────────────────────────────────
        approx_top_count(payment_method, 1)[offset(0)].value
            as preferred_payment_method,

        -- ── Category preferences ────────────────────────────────
        approx_top_count(product_category, 1)[offset(0)].value
            as favorite_category

    from orders
    group by customer_id

),

-- Join customer profile + order metrics
enriched as (

    select
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.state,
        c.country,
        c.age,
        c.age_bucket,
        c.gender,
        c.registration_date,
        c.is_active,

        -- ── Order metrics (default 0 if customer never ordered) ─
        coalesce(co.total_spend, 0)               as total_spend,
        coalesce(co.order_count, 0)                as order_count,
        coalesce(co.avg_order_value, 0)            as avg_order_value,
        coalesce(co.total_units_purchased, 0)      as total_units_purchased,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.days_since_last_order, 9999)   as days_since_last_order,
        coalesce(co.customer_tenure_days, 0)       as customer_tenure_days,
        coalesce(co.orders_per_month, 0)           as orders_per_month,
        co.preferred_payment_method,
        co.favorite_category,

        -- ── LTV segment based on total spend ────────────────────
        case
            when coalesce(co.total_spend, 0) = 0
                then 'New'
            when co.total_spend >= 50000
                then 'High'
            when co.total_spend >= 10000
                then 'Medium'
            else 'Low'
        end as ltv_segment,

        -- ── Churn risk indicator ────────────────────────────────
        -- Based on recency: no order in 90+ days = high risk
        case
            when coalesce(co.order_count, 0) = 0
                then 'no_orders'
            when co.days_since_last_order <= 30
                then 'active'
            when co.days_since_last_order <= 60
                then 'at_risk'
            when co.days_since_last_order <= 90
                then 'slipping'
            else 'churned'
        end as churn_risk,

        -- ── RFM-style score (1–5 each dimension) ────────────────
        ntile(5) over (order by coalesce(co.days_since_last_order, 9999) desc)
            as recency_score,
        ntile(5) over (order by coalesce(co.order_count, 0) asc)
            as frequency_score,
        ntile(5) over (order by coalesce(co.total_spend, 0) asc)
            as monetary_score

    from current_customers c
    left join customer_orders co
        on c.customer_id = co.customer_id

)

select
    *,

    -- ── Combined RFM score ──────────────────────────────────
    (recency_score + frequency_score + monetary_score)
        as rfm_total_score

from enriched
