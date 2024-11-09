{# So this does the merge insert adapter.dispatch will call the correct function based on the adapter
In our current case we are using trino so we change it to trino__ #}
{% macro scd2_merge_sql(target, source, insert_cols) -%}
  {{ adapter.dispatch('scd2_merge_sql', 'dbt')(target, source, insert_cols) }}
{%- endmacro %}


{% macro trino__scd2_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to = date'9999-12-31'
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})

{% endmacro %}