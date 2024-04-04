{% macro db2_for_i__validate_get_incremental_strategy(config) %}
    {# -- Find and validate the incremental strategy  #}
    {% set strategy = config.get("incremental_strategy", default="merge") %}

    {% set invalid_strategy_msg -%}
        Invalid incremental strategy provided: {{ strategy }}
        Expected one of: 'merge', 'delete_insert', 'insert_overwrite'
    {%- endset %}
    {% if strategy not in ['merge', 'delete_insert', 'insert_overwrite'] %}
        {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}

    {% do return(strategy) %}
{% endmacro %}




{% macro db2_for_i__get_incremental_sql(strategy, strategy_arg_dict) %}
    {% if strategy == 'merge' %}
        {% do return(get_merge_sql(**strategy_arg_dict)) %}
    {% elif strategy == 'delete_insert' %}
        {% do return(get_delete_insert_merge_sql(**strategy_arg_dict)) %}
    {% elif strategy == 'insert_overwrite' %}
        {% do return(get_insert_overwrite_merge_sql(**strategy_arg_dict)) %}
    {% else %}
        {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
    {% endif %}
{% endmacro %}




{% materialization incremental, adapter='db2_for_i' -%}

    -- relations
    {%- set existing_relation = load_cached_relation(this) -%}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set temp_relation = make_temp_relation(target_relation) -%}
    {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
    {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

    -- configs
    {%- set unique_key = config.get('unique_key') -%}
    {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
    {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

    -- validate incremental strategy
    {%- set incremental_strategy = db2_for_i__validate_get_incremental_strategy(config) -%}

    -- the temp_ and backup_ relations should not already exist in the database; get_relation will 
    -- return None in that case. Otherwise, we get a relation that we can drop later, before we try 
    -- to use this name for the current operation. This has to happen before BEGIN, in a separate transaction.
    {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

    -- grab current tables grant configs for comparison later on
    {%- set grant_config = config.get('grants') -%}
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set to_drop = [] %}
    {% if existing_relation is none %}
        {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
    {% elif full_refresh_mode %}
        {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
        {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
        {% set need_swap = true %}
    {% else %}
        {% do adapter.drop_relation(temp_relation) %}
        {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}
        {% do to_drop.append(temp_relation) %}
        {% do adapter.expand_target_column_types(
                from_relation=temp_relation,
                to_relation=target_relation
            ) 
        %}
        {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
        {% if not dest_columns %}
            {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
        {% endif %}
        {% set incremental_predicates = config.get('incremental_predicates', none) %}
        {% set strategy_arg_dict = ({'target': target_relation, 'source': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'predicates': incremental_predicates}) %}
        {% set build_sql = db2_for_i__get_incremental_sql(incremental_strategy, strategy_arg_dict) %}
        
    {% endif %}
    
    {% call statement('main') %}
        {{ build_sql }}
    {% endcall %}

    {% if need_swap %}
        {% do adapter.rename_relation(target_relation, backup_relation) %}
        {% do adapter.rename_relation(intermediate_relation, target_relation) %}
        {% do to_drop.append(backup_relation) %}
    {% endif %}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
        {% do create_indexes(target_relation) %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {% do adapter.commit() %}

    {% for rel in to_drop %}
        {{ log("Dropping relation " ~ rel) }}
        {% do adapter.drop_relation(rel) %}
    {% endfor %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
