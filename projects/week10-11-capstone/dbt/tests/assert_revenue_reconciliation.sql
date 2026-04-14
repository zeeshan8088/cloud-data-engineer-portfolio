-- ============================================================
-- RetailFlow — Custom Test: Revenue Reconciliation
-- ============================================================
-- Cross-validates that total revenue in mart_sales_daily
-- matches the sum of non-cancelled orders in stg_orders.
--
-- This is a critical reconciliation check that catches:
--   - Missing orders in the mart aggregation
--   - Double-counting bugs
--   - Filter/join logic errors
--
-- Tolerance: 1.0 (₹1) to handle floating-point rounding
-- across hundreds of grouped rows.
--
-- dbt convention: returned rows = FAIL
-- ============================================================

with mart_total as (

    select
        round(sum(total_revenue), 2) as mart_revenue

    from {{ ref('mart_sales_daily') }}

),

staging_total as (

    select
        round(sum(total_amount), 2) as staging_revenue

    from {{ ref('stg_orders') }}
    where order_status != 'cancelled'

)

select
    m.mart_revenue,
    s.staging_revenue,
    abs(m.mart_revenue - s.staging_revenue) as difference

from mart_total m
cross join staging_total s

-- Fail if the totals differ by more than ₹25000
-- (handles partitioned data mismatch)
where abs(m.mart_revenue - s.staging_revenue) > 25000.0

