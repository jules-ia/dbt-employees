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
    
    {% for column in columns %}
      {% set tag_sql %}
        ALTER {{ relation_type }} {{ this }} 
        ALTER COLUMN {{ column }} SET TAG {{ tag_name }} = '{{ masking_domain }}'
      {% endset %}
      {{ log("Setting " ~ relation_type ~ " tag on column " ~ column ~ " with domain '" ~ masking_domain ~ "'", info=true) }}
      {% do run_query(tag_sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}