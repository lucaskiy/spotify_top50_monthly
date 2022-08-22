{% macro ms_to_minutes(column_name) -%}
  FORMAT_TIME('%M:%S', extract (TIME from TIMESTAMP_MILLIS( {{column_name}} )))
{%- endmacro %}
