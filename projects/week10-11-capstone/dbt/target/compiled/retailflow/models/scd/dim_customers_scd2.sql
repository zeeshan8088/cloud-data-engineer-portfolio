-- ============================================================
-- RetailFlow — dim_customers_scd2 (Silver SCD Layer)
-- ============================================================
-- Production-grade SCD Type 2 customer dimension table.
-- Built on top of the customers_snapshot dbt snapshot.
--
-- Each row = one VERSION of a customer record.
-- When tracked columns change (email / phone / city / state /
-- is_active), dbt snapshot inserts a NEW version and closes
-- the old one with dbt_valid_to.
--
-- KEY COLUMNS:
--   _valid_from    → when this version became active
--   _valid_to      → when superseded (NULL if current)
--   _is_current    → boolean convenience flag
--   _version_num   → sequential version counter per customer
--   customer_version_sk → unique surrogate per version
--
-- MATERIALIZATION: incremental (only process new/changed rows)
-- UNIQUE KEY:       customer_version_sk
-- ============================================================



with snapshot_data as (

    select * from `intricate-ward-459513-e1`.`retailflow_silver`.`customers_snapshot`

    
    -- Incremental logic: only re-process snapshot rows that were
    -- inserted or updated since our last dim_customers_scd2 run.
    -- We compare dbt_updated_at (snapshot-managed timestamp) against
    -- the max _dbt_updated_at already present in this table.
    where dbt_updated_at > (
        select coalesce(
            max(_dbt_updated_at),
            timestamp('2000-01-01')
        )
        from `intricate-ward-459513-e1`.`retailflow_silver`.`dim_customers_scd2`
    )
    

),

versioned as (

    select
        -- ── Surrogate key: unique per customer × version ──────
        -- Combines customer_id + dbt's own scd_id so each
        -- historical row gets a stable, globally unique key.
        to_hex(md5(cast(coalesce(cast(customer_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dbt_scd_id as string), '_dbt_utils_surrogate_key_null_') as string)))
                                                        as customer_version_sk,

        -- ── Natural key ──────────────────────────────────────
        customer_id,

        -- ── Name & contact ───────────────────────────────────
        full_name,
        first_name,
        last_name,
        email,
        phone,

        -- ── Location ─────────────────────────────────────────
        city,
        state,
        country,

        -- ── Demographics ─────────────────────────────────────
        age,
        case
            when age between 18 and 25 then '18-25'
            when age between 26 and 35 then '26-35'
            when age between 36 and 45 then '36-45'
            when age between 46 and 55 then '46-55'
            when age > 55             then '56+'
            else 'unknown'
        end                                             as age_bucket,
        gender,

        -- ── Account ──────────────────────────────────────────
        registration_date,
        is_active,

        -- ── SCD Type 2 validity columns ───────────────────────
        -- dbt_valid_from / dbt_valid_to are columns that dbt
        -- snapshot automatically maintains. We rename them to
        -- our own convention for cleaner downstream SQL.
        dbt_valid_from                                  as _valid_from,
        dbt_valid_to                                    as _valid_to,

        -- Convenience boolean so analysts can do:
        --   WHERE _is_current = true
        -- instead of: WHERE dbt_valid_to IS NULL
        case
            when dbt_valid_to is null then true
            else false
        end                                             as _is_current,

        -- Version counter: V1 = original, V2 = first change, etc.
        -- Window function over all versions ordered by valid_from.
        row_number() over (
            partition by customer_id
            order by dbt_valid_from asc
        )                                               as _version_num,

        -- ── dbt internal metadata (kept for debugging) ────────
        dbt_scd_id                                      as _scd_id,
        dbt_updated_at                                  as _dbt_updated_at,

        -- ── Audit ─────────────────────────────────────────────
        ingested_at,
        batch_id

    from snapshot_data

)

select * from versioned