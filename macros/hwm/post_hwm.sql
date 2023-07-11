{% macro post_hwm(this) %}
{% if not flags.FULL_REFRESH %}
CROSS JOIN HWM_MAX 
WHERE ldts > hwm_max.hwm_max_ts
{%- endif -%}
{% endmacro %}