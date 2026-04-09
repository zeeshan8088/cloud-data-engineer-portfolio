-- ============================================================
-- RetailFlow — mart_demand_forecast (Gold Layer)  [SCAFFOLD]
-- ============================================================
-- Joins Vertex AI batch prediction output back to product
-- names and catalog data for Looker Studio dashboards.
--
-- Source: retailflow_predictions.demand_forecast (Vertex AI)
--         stg_products (product catalog)
--
-- This model will be activated on Day 9 after batch_predict.py
-- writes predictions to the demand_forecast table.
--
-- Grain: one row per product
-- Materialized as: TABLE
-- ============================================================

{#  ═══════════════════════════════════════════════════════════
    DAY 9 ACTIVATION:
    Replace the placeholder SELECT at the bottom with the
    full model below. Uncomment the config block and the
    full SQL query.

    config(
        materialized='table',
        tags=['gold', 'mart', 'forecast', 'vertex_ai']
    )

    with predictions as (

        select
            product_id,
            predicted_units,
            forecast_generated_at,
            forecast_for_week_starting
        from the predictions source table

    ),

    products as (

        select * from stg_products ref

    ),

    enriched as (

        select
            p.product_id,
            p.product_sk,
            p.product_name,
            p.category,
            p.category_display,
            p.price                              as list_price,
            p.cost_price,
            p.margin_pct,
            p.stock_quantity,
            p.stock_status,
            p.rating,
            p.review_count,
            p.supplier,

            -- Prediction columns
            pred.predicted_units,
            pred.forecast_generated_at,
            pred.forecast_for_week_starting,

            -- Derived: projected revenue
            round(pred.predicted_units * p.price, 2)
                as projected_revenue,

            -- Derived: can we fulfill the forecast?
            case
                when p.stock_quantity >= pred.predicted_units
                    then 'sufficient'
                when p.stock_quantity >= pred.predicted_units * 0.5
                    then 'at_risk'
                else 'insufficient'
            end as stock_adequacy,

            -- Derived: units to reorder
            greatest(
                pred.predicted_units - p.stock_quantity,
                0
            ) as recommended_reorder_qty

        from predictions pred
        inner join products p
            on pred.product_id = p.product_id

    )

    select * from enriched
    ═══════════════════════════════════════════════════════════ #}

-- Placeholder until Day 9 activation
SELECT 1 AS placeholder
