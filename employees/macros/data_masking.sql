{% macro set_column_tags(columns, domain=none, tag_name='DATAMESH.SQLMESH.VARCHAR_MASKING') %}
  {% if target.type == 'snowflake' and execute %}
    {% set masking_domain = domain or var('domain', 'employees') %}
    {% set relation_type = 'TABLE' if config.get('materialized') == 'table' else 'VIEW' %}
    
    {% if columns is string %}
      {# If columns is a single string, convert to list #}
      {% set columns = [columns] %}
    {% elif columns is mapping %}
      {# If columns is a dict (old format), extract keys #}
      {% set columns = columns.keys() | list %}
    {% endif %}
    
    {# Build all ALTER statements as a single SQL block #}
    {% set all_tag_sql = [] %}
    {% for column in columns %}
      {% set tag_statement = "ALTER " ~ relation_type ~ " " ~ this ~ " ALTER COLUMN " ~ column ~ " SET TAG " ~ tag_name ~ " = '" ~ masking_domain ~ "'" %}
      {% do all_tag_sql.append(tag_statement) %}
      {{ log("Preparing tag for column " ~ column ~ " with domain '" ~ masking_domain ~ "'", info=true) }}
    {% endfor %}
    
    {# Execute all statements as one #}
    {% if all_tag_sql %}
      {% set combined_sql = all_tag_sql | join(';\n') %}
      {% do run_query(combined_sql) %}
    {% endif %}
  {% endif %}
{% endmacro %}