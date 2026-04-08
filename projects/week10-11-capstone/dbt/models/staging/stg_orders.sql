-- ============================================================
-- RetailFlow — stg_orders (Silver Layer)
-- ============================================================
-- Cleans, deduplicates, and enriches raw order data.
-- Dedup strategy: keep the LATEST ingestion of each order_id.
-- Adds: order_sk (surrogate key), order_date_date (DATE cast)
-- Materialized as: VIEW (see dbt_project.yml)
-- ============================================================

with source as (

    select * from {{ source('bronze', 'raw_orders') }}

),

-- Dedup: keep latest version of each order_id
deduplicated as (

    select
        *,
        row_number() over (
            partition by order_id
            order by _ingested_at desc
        ) as _rn

    from source

),

cleaned as (

    select
        -- ── Primary key ─────────────────────────────────────
        {{ dbt_utils.generate_surrogate_key(['order_id']) }}
            as order_sk,
        cast(order_id as INT64)
            as order_id,

        -- ── Foreign keys ────────────────────────────────────
        cast(user_id as INT64)
            as customer_id,
        cast(product_id as INT64)
            as product_id,

        -- ── Order details ───────────────────────────────────
        trim(product_title)
            as product_title,
        lower(trim(product_category))
            as product_category,
        cast(product_price as FLOAT64)
            as unit_price,
        cast(quantity as INT64)
            as quantity,
        cast(total_amount as FLOAT64)
            as total_amount,

        -- ── Status & payment ────────────────────────────────
        lower(trim(status))
            as order_status,
        lower(trim(payment_method))
            as payment_method,

        -- ── Dates ───────────────────────────────────────────
        cast(order_date as TIMESTAMP)
            as order_timestamp,
        date(cast(order_date as TIMESTAMP))
            as order_date,

        -- ── Shipping ────────────────────────────────────────
        trim(shipping_address)
            as shipping_address,

        -- ── Audit columns ───────────────────────────────────
        cast(_ingested_at as TIMESTAMP)
            as ingested_at,
        _source                     as record_source,
        _batch_id                   as batch_id,
        _ingested_date              as ingested_date

    from deduplicated
    where _rn = 1

)

select * from cleaned
