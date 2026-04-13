-- ============================================================
-- RetailFlow — mart_funnel (Gold Layer)
-- ============================================================
-- Conversion funnel analysis from clickstream events.
-- Designed for Looker Studio "Funnel Analysis" dashboard page.
--
-- Grain: one row per calendar date
-- Sources: stg_clickstream, stg_orders
--
-- Funnel steps:
--   page_view → product_view → add_to_cart →
--   checkout_started → purchase
--
-- Key metrics:
--   step counts, step-to-step conversion rates,
--   overall view_to_order_rate, sessions, bounce stats
--
-- Partitioned by: event_date (day)
-- Materialized as: TABLE
-- ============================================================

{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        tags=['gold', 'mart', 'funnel']
    )
}}

with clickstream as (

    select * from {{ ref('stg_clickstream') }}

),

orders as (

    select * from {{ ref('stg_orders') }}
    where order_status != 'cancelled'

),

-- ── Daily clickstream funnel counts ─────────────────────────
daily_clicks as (

    select
        event_date,

        -- Total events
        count(*)                                    as total_events,

        -- Unique sessions
        count(distinct session_id)                  as total_sessions,

        -- Unique visitors
        count(distinct user_id)                     as unique_visitors,

        -- ── Funnel step counts ──────────────────────────────────
        countif(event_type = 'page_view')           as page_views,
        countif(event_type = 'product_view')        as product_views,
        countif(event_type = 'add_to_cart')         as add_to_cart,
        countif(event_type = 'checkout_started')    as checkout_started,
        countif(event_type = 'purchase')            as purchases,

        -- ── Sessions by deepest funnel step reached ─────────────
        count(distinct case when event_type = 'purchase'
                            then session_id end)
            as sessions_with_purchase,

        count(distinct case when event_type = 'add_to_cart'
                            then session_id end)
            as sessions_with_cart,

        -- ── Device breakdown ────────────────────────────────────
        countif(device_type = 'mobile')             as mobile_events,
        countif(device_type = 'desktop')            as desktop_events,
        countif(device_type = 'tablet')             as tablet_events

    from clickstream
    group by event_date

),

-- ── Daily order counts (from orders table) ──────────────────
daily_orders as (

    select
        order_date                                  as event_date,
        count(distinct order_id)                    as confirmed_orders,
        round(sum(total_amount), 2)                 as order_revenue

    from orders
    group by order_date

),

-- ── Join and compute conversion rates ───────────────────────
funnel as (

    select
        dc.event_date,

        -- ── Raw counts ──────────────────────────────────────────
        dc.total_events,
        dc.total_sessions,
        dc.unique_visitors,
        dc.page_views,
        dc.product_views,
        dc.add_to_cart,
        dc.checkout_started,
        dc.purchases,
        dc.sessions_with_purchase,
        dc.sessions_with_cart,
        coalesce(do_agg.confirmed_orders, 0)        as confirmed_orders,
        coalesce(do_agg.order_revenue, 0)           as order_revenue,

        -- ── Device split ────────────────────────────────────────
        dc.mobile_events,
        dc.desktop_events,
        dc.tablet_events,

        -- ── Step-to-step conversion rates (%) ───────────────────
        round(safe_divide(dc.product_views, dc.page_views) * 100, 2)
            as view_to_product_rate,

        round(safe_divide(dc.add_to_cart, dc.product_views) * 100, 2)
            as product_to_cart_rate,

        round(safe_divide(dc.checkout_started, dc.add_to_cart) * 100, 2)
            as cart_to_checkout_rate,

        round(safe_divide(dc.purchases, dc.checkout_started) * 100, 2)
            as checkout_to_purchase_rate,

        -- ── Overall conversion rate ─────────────────────────────
        round(safe_divide(dc.purchases, dc.page_views) * 100, 2)
            as view_to_order_rate,

        -- ── Session conversion rate ─────────────────────────────
        round(safe_divide(dc.sessions_with_purchase,
                          dc.total_sessions) * 100, 2)
            as session_conversion_rate,

        -- ── Average events per session ──────────────────────────
        round(safe_divide(dc.total_events, dc.total_sessions), 1)
            as avg_events_per_session,

        -- ── Cart abandonment rate ───────────────────────────────
        round(safe_divide(
            dc.add_to_cart - dc.checkout_started,
            greatest(dc.add_to_cart, 1)
        ) * 100, 2)
            as cart_abandonment_rate

    from daily_clicks dc
    left join daily_orders do_agg
        on dc.event_date = do_agg.event_date

)

select * from funnel
