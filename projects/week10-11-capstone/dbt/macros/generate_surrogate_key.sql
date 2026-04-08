-- ============================================================
-- RetailFlow — generate_surrogate_key macro
-- ============================================================
-- Convenience wrapper around dbt_utils.generate_surrogate_key.
-- Usage: {{ generate_surrogate_key(['col1', 'col2']) }}
-- ============================================================

{% macro generate_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
