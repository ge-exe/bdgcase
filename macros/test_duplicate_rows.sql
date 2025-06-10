{% macro test_duplicate_rows(model, columns=none) %}

  {% if columns is none %}
    {% set columns = adapter.get_columns_in_relation(model) | map(attribute='name') | list %}
  {% endif %}

  {% set column_list = columns | join(', ') %}

  select
    {{ column_list }},
    count(*) as duplicate_count
  from {{ model }}
  group by {{ column_list }}
  having count(*) > 1
  order by duplicate_count desc

{% endmacro %}