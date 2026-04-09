-- ============================================================
-- RetailFlow — mart_product_performance (Gold Layer)
-- ============================================================
-- Product-level performance analytics table.
-- Designed for Looker Studio "Product Performance" section and
-- also feeds the Vertex AI training data pipeline (Day 8–9).
--
-- Grain: one row per product
-- Sources: stg_orders, stg_products
--
-- Key metrics:
--   units_sold, revenue, avg_selling_price,
--   unique_buyers, return_rate, rank_in_category
--
-- Window functions: RANK() OVER (PARTITION BY category)
-- Materialized as: TABLE
-- ============================================================

{{
    config(
        materialized='table',
        tags=['gold', 'mart', 'product']
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

-- Aggregate order-level metrics per product
product_sales as (

    select
        o.product_id,

        -- ── Volume metrics ──────────────────────────────────────
        sum(case when o.order_status != 'cancelled'
                 then o.quantity else 0 end)
            as units_sold,

        count(distinct case when o.order_status != 'cancelled'
                            then o.order_id end)
            as order_count,

        count(distinct o.customer_id)
            as unique_buyers,

        -- ── Revenue metrics ─────────────────────────────────────
        round(sum(case when o.order_status != 'cancelled'
                       then o.total_amount else 0 end), 2)
            as revenue,

        round(avg(case when o.order_status != 'cancelled'
                       then o.unit_price end), 2)
            as avg_selling_price,

        -- ── Cancellation as proxy for returns ───────────────────
        -- In our synthetic data, cancelled orders approximate
        -- return behavior. In production, a dedicated returns
        -- table would replace this metric.
        countif(o.order_status = 'cancelled')
            as cancelled_count,

        round(safe_divide(
            countif(o.order_status = 'cancelled'),
            count(distinct o.order_id)
        ) * 100, 2)
            as return_rate,

        -- ── Time-based metrics ──────────────────────────────────
        min(o.order_date) as first_sale_date,
        max(o.order_date) as last_sale_date,
        count(distinct o.order_date) as active_sale_days

    from orders o
    group by o.product_id

),

-- Join product catalog with sales performance
enriched as (

    select
        p.product_id,
        p.product_sk,
        p.product_name,
        p.category,
        p.category_display,
        p.price                              as list_price,
        p.cost_price,
        p.margin,
        p.margin_pct,
        p.stock_quantity,
        p.stock_status,
        p.is_available,
        p.supplier,
        p.rating,
        p.review_count,

        -- ── Sales metrics (default 0 for unsold products) ───────
        coalesce(ps.units_sold, 0)           as units_sold,
        coalesce(ps.order_count, 0)          as order_count,
        coalesce(ps.unique_buyers, 0)        as unique_buyers,
        coalesce(ps.revenue, 0)              as revenue,
        coalesce(ps.avg_selling_price, p.price) as avg_selling_price,
        coalesce(ps.cancelled_count, 0)      as cancelled_count,
        coalesce(ps.return_rate, 0)          as return_rate,
        ps.first_sale_date,
        ps.last_sale_date,
        coalesce(ps.active_sale_days, 0)     as active_sale_days,

        -- ── Profitability ───────────────────────────────────────
        round(coalesce(ps.revenue, 0) -
              (coalesce(ps.units_sold, 0) * p.cost_price), 2)
            as gross_profit,

        -- ── Revenue per unit for AI features ────────────────────
        round(safe_divide(
            coalesce(ps.revenue, 0),
            greatest(coalesce(ps.units_sold, 0), 1)
        ), 2) as revenue_per_unit,

        -- ── Inventory health ────────────────────────────────────
        -- Days of stock remaining at current sell rate
        round(safe_divide(
            p.stock_quantity,
            safe_divide(coalesce(ps.units_sold, 0),
                        greatest(coalesce(ps.active_sale_days, 0), 1))
        ), 1) as estimated_days_of_stock

    from products p
    left join product_sales ps
        on p.product_id = ps.product_id

),

-- ── Window functions: within-category ranking ───────────────
ranked as (

    select
        *,

        -- Rank by revenue within category
        rank() over (
            partition by category
            order by revenue desc
        ) as rank_in_category,

        -- Rank by units sold within category
        rank() over (
            partition by category
            order by units_sold desc
        ) as units_rank_in_category,

        -- Percentage of category revenue
        round(safe_divide(
            revenue,
            sum(revenue) over (partition by category)
        ) * 100, 2)
            as pct_of_category_revenue,

        -- Overall revenue rank
        rank() over (order by revenue desc)
            as overall_revenue_rank

    from enriched

)

select * from ranked
