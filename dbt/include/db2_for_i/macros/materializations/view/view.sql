{%- materialization view, adapter='db2_for_i' -%}
 
 
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
 
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "existing_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...
     We need to set the type of this relation to be the type of the existing_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the existing_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
 
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}
 
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
 
  -- cleanup
  {% if existing_relation is not none %}
    -- >>> DB2 Note: Special case if existing relation is view -> drop
    {% if existing_relation.is_view %}
        {{ adapter.drop_relation(existing_relation) }}
    {% else %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
    -- <<<
  {% endif %}
 
  -- build model
  -- >>> DB2 Note: Cannot rename view, create target directly but after cleanup
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}
  -- <<<
 
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
 
  {% do persist_docs(target_relation, model) %}
 
  {{ run_hooks(post_hooks, inside_transaction=True) }}
 
  {{ adapter.commit() }}
 
  {{ drop_relation_if_exists(backup_relation) }}
 
  {{ run_hooks(post_hooks, inside_transaction=False) }}
 
  {{ return({'relations': [target_relation]}) }}
 
{%- endmaterialization -%}