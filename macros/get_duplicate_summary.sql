{% macro get_duplicate_summary(relation_name, key_columns) %}

  {% set column_list = key_columns | join(', ') %}
  
  select
    '{{ relation_name }}' as table_name,
    count(*) as total_rows,
    count(distinct {{ column_list }}) as distinct_key_combinations,
    count(*) - count(distinct {{ column_list }}) as duplicate_rows,
    case 
      when count(*) = count(distinct {{ column_list }}) then 'No Duplicates'
      else 'Has Duplicates'
    end as duplicate_status
  from {{ relation_name }}

{% endmacro %}