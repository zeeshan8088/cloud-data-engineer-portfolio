

  create or replace view `intricate-ward-459513-e1`.`retailflow_silver`.`stg_clickstream`
  OPTIONS()
  as -- ============================================================
-- RetailFlow — stg_clickstream (Silver Layer)
-- ============================================================
-- Cleans, deduplicates, and enriches raw clickstream events.
-- Dedup: keep latest ingestion of each event_id.
-- Adds: event_date, is_conversion, funnel_step_number
-- Materialized as: VIEW
-- ============================================================

with source as (

    select * from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_clickstream`

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at desc
        ) as _rn

    from source

),

cleaned as (

    select
        -- ── Primary key ─────────────────────────────────────
        to_hex(md5(cast(coalesce(cast(event_id as string), '_dbt_utils_surrogate_key_null_') as string)))
            as event_sk,
        trim(event_id)
            as event_id,

        -- ── Session & user ──────────────────────────────────
        trim(session_id)
            as session_id,
        cast(user_id as INT64)
            as user_id,

        -- ── Event classification ────────────────────────────
        lower(trim(event_type))
            as event_type,

        -- Numeric funnel position for easy ordering/filtering
        case lower(trim(event_type))
            when 'page_view'        then 1
            when 'product_view'     then 2
            when 'add_to_cart'      then 3
            when 'checkout_started' then 4
            when 'purchase'         then 5
            else 0
        end as funnel_step_number,

        -- Boolean: did this event lead to revenue?
        lower(trim(event_type)) = 'purchase'
            as is_conversion,

        -- ── Page & product ──────────────────────────────────
        trim(page_url)
            as page_url,
        cast(product_id as INT64)
            as product_id,

        -- ── Timestamps ──────────────────────────────────────
        cast(event_timestamp as TIMESTAMP)
            as event_timestamp,
        date(cast(event_timestamp as TIMESTAMP))
            as event_date,

        -- ── Device & traffic ────────────────────────────────
        lower(trim(device_type))
            as device_type,
        trim(browser)
            as browser,
        trim(referrer)
            as referrer,

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

