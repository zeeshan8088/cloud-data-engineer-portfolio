-- ============================================================
-- RetailFlow — customers_snapshot (SCD Type 2)
-- ============================================================
-- Tracks historical changes to customer records using dbt's
-- built-in snapshot mechanism (strategy: check).
--
-- HOW IT WORKS:
--   1. First run   → copies all rows, sets dbt_valid_from = now,
--                    dbt_valid_to = null, _is_current = true
--   2. Later runs  → compares check_cols for each customer_id.
--                    If ANY tracked column changed:
--                      • Old row: dbt_valid_to = change timestamp
--                      • New row: dbt_valid_from = change timestamp,
--                                 dbt_valid_to = null (current)
--
-- TRACKED COLUMNS (trigger a new version when changed):
--   email, phone, city, state, is_active
--
-- NOT TRACKED (changes are silently overwritten):
--   first_name, last_name — typo fixes don't need a history row
-- ============================================================

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='retailflow_silver',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email', 'phone', 'city', 'state', 'is_active'],
        invalidate_hard_deletes=True,
    )
}}

-- Pull from Bronze and apply light cleaning so snapshot stores
-- clean values (avoids storing raw messy strings in history).
select
    -- ── Primary key ──────────────────────────────────────────
    cast(customer_id as INT64)                              as customer_id,

    -- ── Name ─────────────────────────────────────────────────
    initcap(trim(first_name))                               as first_name,
    initcap(trim(last_name))                                as last_name,
    initcap(trim(first_name)) || ' ' || initcap(trim(last_name))
                                                            as full_name,

    -- ── Contact (tracked — changes create new SCD2 version) ──
    lower(trim(email))                                      as email,
    trim(phone)                                             as phone,

    -- ── Location (tracked) ───────────────────────────────────
    initcap(trim(city))                                     as city,
    initcap(trim(state))                                    as state,
    initcap(trim(country))                                  as country,

    -- ── Demographics ─────────────────────────────────────────
    cast(age as INT64)                                      as age,
    trim(gender)                                            as gender,

    -- ── Account status (tracked) ─────────────────────────────
    cast(registration_date as DATE)                         as registration_date,
    cast(is_active as BOOL)                                 as is_active,

    -- ── Audit ────────────────────────────────────────────────
    cast(_ingested_at as TIMESTAMP)                         as ingested_at,
    _batch_id                                               as batch_id

from {{ source('bronze', 'raw_customers') }}

-- De-duplicate Bronze before snapshotting: keep the most
-- recently ingested record per customer_id so duplicate
-- ingestion batches don't create phantom SCD2 versions.
qualify row_number() over (
    partition by customer_id
    order by _ingested_at desc
) = 1

{% endsnapshot %}
