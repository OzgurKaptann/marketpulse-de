{% macro postgres__get_create_view_as_sql(relation, sql) -%}
  create or replace view {{ relation }} as
  {{ sql }}
{%- endmacro %}
