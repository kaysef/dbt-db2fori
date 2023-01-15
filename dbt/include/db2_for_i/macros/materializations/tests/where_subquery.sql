{% macro get_where_subquery(relation) %}
    {% do return(adapter.dispatch('get_where_subquery', 'dbt')(relation)) %}
{% endmacro %}


{% macro db2_for_i__get_where_subquery(relation) %}
    {% set where = config.get('where', '') %}
    {% if where %}
        {%- set filtered -%}
            (SELECT * FROM {{ relation }} WHERE {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {% else %}
        {% do return(relation) %}
    {%- endif -%}
{% endmacro %}