{# This is a call to the macro and will call the right one based on adapter being used  #}
{% macro build_snapshot_table(strategy, sql) -%}
  {{ adapter.dispatch('build_snapshot_table', 'dbt')(strategy, sql) }}
{% endmacro %}

{# trino version based on adapter #}
{% macro trino__build_snapshot_table(strategy, sql) %}

    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}