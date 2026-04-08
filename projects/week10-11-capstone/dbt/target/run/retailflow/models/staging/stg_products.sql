

  create or replace view `intricate-ward-459513-e1`.`retailflow_silver`.`stg_products`
  OPTIONS()
  as -- ============================================================
-- RetailFlow — stg_products (Silver Layer)
-- ============================================================
-- Cleans, deduplicates, and enriches raw product catalog.
-- Dedup: keep latest ingestion of each product_id.
-- Adds: margin, margin_pct, product_sk, category_display
-- Materialized as: VIEW
-- ============================================================

with source as (

    select * from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_products`

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by product_id
            order by _ingested_at desc
        ) as _rn

    from source

),

cleaned as (

    select
        -- ── Primary key ─────────────────────────────────────
        to_hex(md5(cast(coalesce(cast(product_id as string), '_dbt_utils_surrogate_key_null_') as string)))
            as product_sk,
        cast(product_id as INT64)
            as product_id,

        -- ── Product details ─────────────────────────────────
        trim(product_name)
            as product_name,

        -- Raw category (snake_case, for joins)
        lower(trim(category))
            as category,

        -- Display-friendly category name
        replace(initcap(replace(trim(category), '_', ' ')), ' ', ' ')
            as category_display,

        -- ── Pricing ─────────────────────────────────────────
        cast(price as FLOAT64)
            as price,
        cast(cost_price as FLOAT64)
            as cost_price,

        -- Profit margin (absolute)
        round(cast(price as FLOAT64) - cast(cost_price as FLOAT64), 2)
            as margin,

        -- Profit margin (percentage)
        round(
            safe_divide(
                cast(price as FLOAT64) - cast(cost_price as FLOAT64),
                cast(price as FLOAT64)
            ) * 100,
            2
        )   as margin_pct,

        -- ── Inventory ───────────────────────────────────────
        cast(stock_quantity as INT64)
            as stock_quantity,

        case
            when stock_quantity = 0        then 'out_of_stock'
            when stock_quantity <= 20      then 'low_stock'
            when stock_quantity <= 200     then 'in_stock'
            else                               'overstocked'
        end as stock_status,

        cast(is_available as BOOL)
            as is_available,

        -- ── Supplier ────────────────────────────────────────
        trim(supplier)
            as supplier,

        -- ── Ratings ─────────────────────────────────────────
        cast(rating as FLOAT64)
            as rating,
        cast(review_count as INT64)
            as review_count,

        -- ── Audit columns ───────────────────────────────────
        cast(_ingested_at as TIMESTAMP)
            as ingested_at,
        _source                     as record_source,
        _batch_id                   as batch_id,
        _ingested_date              as ingested_date

    from deduplicated
    where _rn = 1

)

select * from cleaned;

