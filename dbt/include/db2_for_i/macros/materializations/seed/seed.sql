{# -- Macros to help the seed materialization -- #}
{% macro create_csv_table(model, agate_table) -%}
  {{ adapter.dispatch('create_csv_table')(model, agate_table) }}
{%- endmacro %}

{% macro reset_csv_table(model, full_refresh, old_relation, agate_table) -%}
  {{ adapter.dispatch('reset_csv_table')(model, full_refresh, old_relation, agate_table) }}
{%- endmacro %}

{% macro load_csv_rows(model, agate_table) -%}
  {{ adapter.dispatch('load_csv_rows')(model, agate_table) }}
{%- endmacro %}

{% macro db2_for_i__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{% macro db2_for_i__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = old_relation.quote(database=false, schema=false, identifier=false) -%}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model, agate_table) %}
    {% else %}
        {{ adapter.truncate_relation(old_relation) }}
        {% set sql = "truncate table " ~ old_relation %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}


{% macro get_seed_column_quoted_csv(model, column_names) %}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(adapter.quote_seed_column(col, quote_seed_column)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}


{% macro calc_batch_size(num_columns,max_batch_size) %}
    {#
        DB2 has a limit on the number of parameters of parameters in a single statement.
        Using a limit of 2001, (same as SQL Server for now)
    #}
    {% if num_columns * max_batch_size < 2100 %}
    {% set batch_size = max_batch_size %}
    {% else %}
    {% set batch_size = (2100 / num_columns)|int %}
    {% endif %}

    {{ return(batch_size) }}
{%  endmacro %}


{% macro db2_for_i__basic_load_csv_rows(model, max_batch_size, agate_table) %}

    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}


    {% set batch_size = calc_batch_size(cols_sql|length, max_batch_size) %}
    {% set bindings = [] %}
    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}


        {% set row = None %}
        {% for row in chunk %}
            {% set _ = bindings.extend(row) %}
        {% endfor %}


        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }}) values
 
            {% set row = None %}
            {% for row in chunk -%}
                {% set column = None %}
                ({%- for column in agate_table.column_names -%}
                    ?
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% set _ = adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% set _ = statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% macro db2_for_i__load_csv_rows(model, agate_table) %}
  {% set max_batch_size = var("max_batch_size", 400) %}
  {{ return(db2_for_i__basic_load_csv_rows(model, max_batch_size, agate_table) )}}
{% endmacro %}


{% materialization seed, adapter='db2_for_i' %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}


  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation)) }}
  {% elif exists_as_table %}

    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}

    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}

  {% set code = 'CREATE' if full_refresh_mode else 'INSERT' %}

  {% set rows_affected = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    {{ create_table_sql }};
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {% set target_relation = this.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
