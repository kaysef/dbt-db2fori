
{% macro case_relation_part(quoting, relation_part) %}
  {% if quoting == False %}
    {%- set relation_part = relation_part|upper -%}
  {% endif %}
  {{ return(relation_part) }}
{% endmacro %}


{% macro information_schema_name(database) -%}
  qsys2
{%- endmacro %}


{% macro get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select * from (
            {{ select_sql }}
        ) as dbt_sbq
        WHERE 0 = 1
        FETCH FIRST 0 ROWS ONLY
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro insert_into_from(to_relation, from_relation) -%}
  INSERT INTO {{ to_relation }} SELECT * FROM {{ from_relation }}
{% endmacro %}


{% macro db2_for_i__create_schema(relation) -%}
  {% call statement('create_schema') -%}

    BEGIN
        IF NOT EXISTS (
            SELECT SCHEMA_NAME
            FROM QSYS2.SCHEMATA
            WHERE SCHEMA_NAME = UPPER('{{ relation.without_identifier() }}')
        ) THEN
            PREPARE stmt FROM 'CREATE SCHEMA {{ relation.without_identifier() }}';
            EXECUTE stmt;
        END IF;
    END
  {% endcall %}
{% endmacro %}


{% macro db2_for_i__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    SELECT
        TRIM(LOWER(TABLE_CATALOG)) AS "database",
        TRIM(LOWER(TABLE_NAME)) as "name",
        TRIM(LOWER(TABLE_SCHEMA)) as "schema",
        CASE
          WHEN TABLE_TYPE LIKE '%TABLE%' THEN 'table'
          WHEN TABLE_TYPE = 'VIEW' THEN 'view'
        END AS table_type
    FROM QSYS2.TABLES
    WHERE
        TABLE_SCHEMA = UPPER('{{ schema_relation.schema }}') 
        AND (TABLE_TYPE = 'VIEW' OR TABLE_TYPE LIKE '%TABLE%')
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro db2_for_i__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    SELECT 
        DISTINCT TRIM(SCHEMA_NAME) AS "schema"
    FROM QSYS2.SCHEMATA
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro drop_schema(relation) -%}
  {%- set tables_in_schema_query %}
      SELECT TABLE_NAME FROM QSYS2.TABLES
      WHERE TABLE_SCHEMA = '{{ relation.schema }}'
  {% endset %}
  {% set tables_to_drop = run_query(tables_in_schema_query).columns[0].values() %}
  {% for table in tables_to_drop %}
    {%- set schema_relation = adapter.get_relation(database=relation.database,
                                               schema=relation.schema,
                                               identifier=table) -%}
    {% do drop_relation(schema_relation) %}
  {%- endfor %}

  {% call statement('drop_schema') -%}
    BEGIN
      IF EXISTS (
          SELECT * FROM qsys2.schemata WHERE schema_name = '{{ relation.schema }}'
      ) THEN 
          PREPARE stmt FROM 'DROP SCHEMA {{ relation.schema }}';
          EXECUTE stmt;
      END IF;
    END
  {% endcall %}
{% endmacro %}

{% macro db2_for_i__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    BEGIN
      IF EXISTS (
        SELECT TABLE_NAME
        FROM QSYS2.TABLES
        WHERE TABLE_NAME = UPPER('{{ relation.identifier }}') AND TABLE_SCHEMA = UPPER('{{ relation.schema }}') AND TABLE_TYPE LIKE '%TABLE%'
      ) THEN
        PREPARE stmt FROM 'DROP TABLE {{ relation.include(database=False).quote(schema=False, identifier=False) }}';
        EXECUTE stmt;
      ELSEIF EXISTS (
        SELECT TABLE_NAME
        FROM QSYS2.TABLES
        WHERE TABLE_NAME = UPPER('{{ relation.identifier }}') AND TABLE_SCHEMA = UPPER('{{ relation.schema }}') AND TABLE_TYPE = 'VIEW'
      ) THEN
        PREPARE stmt FROM 'DROP VIEW {{ relation.include(database=False).quote(schema=False, identifier=False) }}';
        EXECUTE stmt;
      END IF;
    END
  {%- endcall %}
{% endmacro %}

{% macro check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    SELECT count(*) as schema_exist FROM qsys2.schemata WHERE schema_name = '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro get_catalog(information_schema, schemas) %}
  {% call statement('get_catalog', fetch_result=True, auto_begin=False) -%}
    SELECT 
        DISTINCT TRIM(SCHEMA_NAME) AS "schema"
    FROM QSYS2.SCHEMATA
  {% endcall %}
  {{ return(load_result('get_catalog').table) }}
{% endmacro %}


{% macro create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  CREATE VIEW {{ relation }} AS
  {{ sql }}

{% endmacro %}

{% macro create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {# Ignore temporary table type #}
  CREATE TABLE {{ relation.quote(schema=False, identifier=False) }}
  AS (
    {{ sql }}
  ) WITH DATA

{%- endmacro %}


{% macro db2_for_i__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
  {% if from_relation.is_table %}
    RENAME TABLE {{ from_relation.quote(schema=False, identifier=False) }} TO {{ to_relation.replace_path(schema=None) }}
  {% endif %}

  {% if from_relation.is_view %}
    {% do exceptions.raise_compiler_error('DB2orI Adapter Error: Renaming of views is not supported') %}
  {% endif %}

  {%- endcall %}
{% endmacro %}


{% macro db2_for_i__current_timestamp() -%}
  CURRENT_TIMESTAMP
{%- endmacro %}

{% macro db2_for_i__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      SELECT
          TRIM(COLUMN_NAME) AS "name",
          TRIM(DATA_TYPE) AS "type",
          CHARACTER_MAXIMUM_LENGTH AS "character_maximum_length",
          NUMERIC_PRECISION AS "numeric_precision",
          NUMERIC_SCALE AS "numeric_scale"
      FROM QSYS2.COLUMNS
      WHERE TABLE_NAME = UPPER('{{ relation.identifier }}')
        {% if relation.schema %}
        AND TABLE_SCHEMA = UPPER('{{ relation.schema }}')
        {% endif %}
      ORDER BY ORDINAL_POSITION

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro db2_for_i__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = 'DBT_TMP__' ~ base_relation.identifier %}
    {% set tmp_relation = base_relation.incorporate(path={"identifier": tmp_identifier}) -%}
    {% do return(tmp_relation) %}
{% endmacro %}


{% macro db2_for_i__alter_relation_comment(relation, comment) %}
  {#-- Whether table or view db2 uses comment on table --#}
  comment on {%- if relation.type == 'view' %} table {% else %} {{ relation.type }} {% endif -%} {{ relation }} is '{{ comment }}'
{% endmacro %}


{% macro db2_for_i__alter_column_comment(relation, column_dict) %}

    COMMENT on COLUMN {{ relation }} (
      {% for column_name in column_dict %}
        {% set comment = column_dict[column_name]['description'] %}
        {{ column_name }} is '{{ comment }}'{%- if not loop.last %}, {% endif -%}
      {% endfor %}
    )
  
{% endmacro %}



{#-- adding short descriptions for relations - a db2 thing --#}
{% macro alter_relation_label(relation, comment) %}
  {#-- Whether table or view db2 uses label on table --#}
  LABEL on {%- if relation.type == 'view' %} table {% else %} {{ relation.type }} {% endif -%} {{ relation }} is '{{ comment }}'
{% endmacro %}

{% macro alter_column_headings(relation, column_dict) %}

    LABEL on COLUMN {{ relation }} (
      {% for column_name in column_dict %}
        {% set comment = column_dict[column_name]['short_description'] %}
        {{ column_name }} is '{{ comment }}'{%- if not loop.last %}, {% endif -%}
      {% endfor %}
    )
  
{% endmacro %}

{% macro alter_column_text(relation, column_dict) %}

    LABEL on COLUMN {{ relation }} (
      {% for column_name in column_dict %}
        {% set comment = column_dict[column_name]['short_description'] %}
        {{ column_name }} TEXT IS '{{ comment }}'{%- if not loop.last %}, {% endif -%}
      {% endfor %}
    )
  
{% endmacro %}



{% macro db2_for_i__persist_docs(relation, model, for_relation, for_columns) -%}
  {# -- Override the persist_docs default behaviour to add the short descriptions --#}

  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if for_relation and config.persist_relation_docs() and model.meta.short_description %}
    {% do run_query(alter_relation_label(relation, model.meta.short_description)) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% do run_query(alter_column_comment(relation, model.columns)) %}
    {% do run_query(alter_column_headings(relation, model.columns)) %}
    {% do run_query(alter_column_text(relation, model.columns)) %}
  {% endif %}

{% endmacro %}


{% macro db2_for_i__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    BEGIN
      ALTER TABLE {{ relation }} ADD column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
      UPDATE {{ relation }} SET {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
      ALTER TABLE {{ relation }} DROP column {{ adapter.quote(column_name) }} cascade;
      ALTER TABLE {{ relation }} rename column {{ adapter.quote(tmp_column) }} TO {{ adapter.quote(column_name) }};
    END
  {% endcall %}

{% endmacro %}
