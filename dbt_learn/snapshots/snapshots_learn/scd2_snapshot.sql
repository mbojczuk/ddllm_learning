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
    {%- set strategy_name = config.get('strategy') -%}

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
        {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
        {% set final_sql = create_table_as(False, target_relation, build_sql) %}

    {% else %}



{% endmaterialization %}