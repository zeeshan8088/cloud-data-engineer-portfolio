-- ============================================================
-- RetailFlow — stg_customers (Silver Layer)
-- ============================================================
-- Cleans, deduplicates, and enriches raw customer data.
-- Dedup: keep latest ingestion of each customer_id.
-- Adds: full_name, email_domain, age_bucket, customer_sk
-- Materialized as: VIEW
-- ============================================================

with source as (

    select * from `intricate-ward-459513-e1`.`retailflow_bronze`.`raw_customers`

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _ingested_at desc
        ) as _rn

    from source

),

cleaned as (

    select
        -- ── Primary key ─────────────────────────────────────
        to_hex(md5(cast(coalesce(cast(customer_id as string), '_dbt_utils_surrogate_key_null_') as string)))
            as customer_sk,
        cast(customer_id as INT64)
            as customer_id,

        -- ── Name ────────────────────────────────────────────
        initcap(trim(first_name))
            as first_name,
        initcap(trim(last_name))
            as last_name,
        initcap(trim(first_name)) || ' ' || initcap(trim(last_name))
            as full_name,

        -- ── Contact ─────────────────────────────────────────
        lower(trim(email))
            as email,
        -- Extract domain: everything after '@'
        lower(
            substr(
                trim(email),
                strpos(trim(email), '@') + 1
            )
        )   as email_domain,
        trim(phone)
            as phone,

        -- ── Location ────────────────────────────────────────
        trim(address)
            as address,
        initcap(trim(city))
            as city,
        initcap(trim(state))
            as state,
        initcap(trim(country))
            as country,

        -- ── Demographics ────────────────────────────────────
        cast(age as INT64)
            as age,
        case
            when age between 18 and 25 then '18-25'
            when age between 26 and 35 then '26-35'
            when age between 36 and 45 then '36-45'
            when age between 46 and 55 then '46-55'
            when age > 55             then '56+'
            else 'unknown'
        end as age_bucket,
        trim(gender)
            as gender,

        -- ── Account ─────────────────────────────────────────
        cast(registration_date as DATE)
            as registration_date,
        cast(is_active as BOOL)
            as is_active,

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