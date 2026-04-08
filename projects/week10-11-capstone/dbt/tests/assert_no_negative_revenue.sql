-- ============================================================
-- RetailFlow — Custom Test: No Negative Revenue
-- ============================================================
-- Singular test that fails if ANY order in stg_orders has a
-- negative total_amount. This catches upstream data corruption.
--
-- dbt convention: if this query returns rows, the test FAILS.
-- ============================================================

select
    order_id,
    total_amount,
    order_status,
    ingested_date

from {{ ref('stg_orders') }}

where total_amount < 0
