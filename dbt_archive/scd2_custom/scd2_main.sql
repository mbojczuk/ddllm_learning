{% materialization scd2_snapshot, adapter='trino' %}

    {# sets config to the config attribute of the model dictionary, 
    which contains configuration parameters for the dbt model being processed #}
    {%- set config = model['config'] -%}
    {# sets target_table to either the alias of the model
    or the name of the model, this determines the final name
    of the table in the database #}
    {%- set target_table = model.get('alias', model.get('name')) -%}

    {# This gets from the config the unique key and sets it #}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set strategy_name = config.get('strategy')|replace(' ', '') lower -%}

    {# The code attempts to retrieve the target table where the snapshot data will be stored #}
    {% set target_relation_exists, target_relation = get_or_create_relation(
            database=model.database,
            schema=model.schema,
            identifier=target_table,
            type='table') -%}

    {%- if not target_relation.is_table -%}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {%- endif -%}

    {# runs any hooks from the configs before the run of the snapshot #}

    {# inside_transaction=False: Runs hooks outside of the transaction scope, allowing for setup tasks that should persist even if the snapshot transaction fails. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {# inside_transaction=True: Runs hooks inside the transaction, ensuring that any pre-setup actions are rolled back if the snapshot fails. #}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# sets correct snapshot strategy based on name #}
    {% set strategy_macro = strategy_dispatch(strategy_name) %}
    {# the strategy specific function/macro call #}
    {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

    {# If the table does not yet exist #}
    {% if not target_relation_exists %}

        {# this will build the sql required to create the snapshot based on the strategy and input #}
        {# build snapshot table is within the helpers #}
        {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
        {# This reference is in the table macro under create #}
        {% set final_sql = create_table_as(False, target_relation, build_sql) %}

    {# If table does exist #}
    {% else %}

        {{ adapter.valid_snapshot_target(target_relation) }}

        {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

        -- this may no-op if the database does not require column expansion
        {% do adapter.expand_target_column_types(from_relation=staging_table,
                                                to_relation=target_relation) %}

        {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                    | rejectattr('name', 'equalto', 'dbt_change_type')
                                    | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                    | rejectattr('name', 'equalto', 'dbt_unique_key')
                                    | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                    | list %}

        {% do create_columns(target_relation, missing_columns) %}

        {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                    | rejectattr('name', 'equalto', 'dbt_change_type')
                                    | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                    | rejectattr('name', 'equalto', 'dbt_unique_key')
                                    | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                    | list %}

        {% set quoted_source_columns = [] %}
        {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
        {% endfor %}

        {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
            )
        %}

    {% endif %}

    {% call statement('main') %}
      {{ final_sql }}
    {% endcall %}
    
{% endmaterialization %}