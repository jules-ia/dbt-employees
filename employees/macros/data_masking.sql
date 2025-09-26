{% macro apply_snowflake_tags() %}
  {% if execute %}
    {% set sql %}
      {% for node in graph.nodes.values() if node.resource_type == 'model' %}
        {% if node.config.get('snowflake_tags') %}
          {% for column, tag_value in node.config.snowflake_tags.items() %}
            ALTER {{ 'TABLE' if node.config.materialized == 'table' else 'VIEW' }} {{ node.relation_name }} 
            ALTER COLUMN {{ column }} SET TAG {{ tag_value }};
          {% endfor %}
        {% endif %}
      {% endfor %}
    {% endset %}
    
    {% if sql.strip() %}
      {{ log("Applying Snowflake tags...", info=true) }}
      {% do run_query(sql) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro set_column_tags(tags_dict) %}
  {% if target.type == 'snowflake' and execute %}
    {% for column, tag in tags_dict.items() %}
      ALTER {{ 'TABLE' if this.is_table else 'VIEW' }} {{ this }} 
      ALTER COLUMN {{ column }} SET TAG {{ tag }};
    {% endfor %}
  {% endif %}
{% endmacro %}