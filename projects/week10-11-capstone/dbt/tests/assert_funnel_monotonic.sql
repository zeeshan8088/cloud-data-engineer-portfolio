-- ============================================================
-- RetailFlow — Custom Test: Funnel Sanity Check
-- ============================================================
-- Validates that purchases never exceed total events on a day,
-- and that no individual funnel step has a negative count.
--
-- NOTE: In synthetic data, step-level monotonicity is not
-- guaranteed because events are randomly sampled. In production
-- with real user sessions, a stricter version of this test
-- would enforce page_views >= product_views >= add_to_cart etc.
--
-- dbt convention: returned rows = FAIL
-- ============================================================

select
    event_date,
    page_views,
    product_views,
    add_to_cart,
    checkout_started,
    purchases,
    total_events

from {{ ref('mart_funnel') }}

where
    -- Any negative count = data corruption
    page_views < 0
    or product_views < 0
    or add_to_cart < 0
    or checkout_started < 0
    or purchases < 0
    -- Purchases exceeding total events is impossible
    or purchases > total_events
