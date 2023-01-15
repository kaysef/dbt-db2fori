{% macro get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    {{ adapter.dispatch('get_test_sql', 'dbt')(main_sql, fail_calc, warn_if, error_if, limit) }}
{% endmacro %}


{% macro db2_for_i__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) %}
    SELECT
        {{ fail_calc }} AS failures,
        CASE WHEN {{ fail_calc }} {{ warn_if }} THEN 1 END AS should_warn,
        CASE WHEN {{ fail_calc }} {{ error_if }} THEN 1 END AS should_error
    FROM (
        {{ main_sql }}
        {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{% endmacro %}