{% macro postgres__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as
  {{ sql }}
{%- endmacro %}
