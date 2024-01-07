{% macro limit_data (time_stamp_column,time_selection,dev_years_of_data=7) -%}
{% if target.name == 'default' %}
where {{time_stamp_column}} >= dateadd('{{time_selection}}',-{{dev_years_of_data}},current_timestamp)
{% endif %}
{%- endmacro %}