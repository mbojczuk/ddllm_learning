{#
    Dispatch strategies by name, optionally qualified to a package
    This macro just tries to concat the name and fine the macro strategy if you have a bunch
    we will use for noww but in the end not really needed
    end result will also be snapshot_stategyname_strategy
#}
{% macro strategy_dispatch(name) -%}
{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        Could not find package '{{package_name}}', called with '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}

  {%- set search_name = 'snapshot_' ~ name ~ '_strategy' -%}

  {% if search_name not in package_context %}
    {% set error_msg %}
        The specified strategy macro '{{name}}' was not found in package '{{ package_name }}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  {{ return(package_context[search_name]) }}
{%- endmacro %}

{#
    Create SCD Hash SQL fields cross-db
#}
{% macro snapshot_hash_arguments(args) -%}
  {{ adapter.dispatch('snapshot_hash_arguments', 'dbt')(args) }}
{%- endmacro %}

{% macro trino__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '||' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{# scd2 core strategy #}
{% macro snapshot_merge_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {# set primary key from config#}
    {% set primary_key = config['unique_key'] %}
    {# need to give the updated_at key for the table #}
    {% set updated_at = config['updated_at'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set row_changed_expr -%}
        ({{ snapshotted_rel }}.dbt_valid_from < {{ current_rel }}.{{ updated_at }})
    {%- endset %}
    {# creating the hash for primary key and updated at value #}
    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes
    }) %}
  
 {% endmacro %}