{% macro insert_hwm(this) %}
INSERT INTO {{ source('LOAD_EXT_META', 'META_HWM') }} (object_name, hwm_ldts, load_date)
                       SELECT '{{ this }}',MAX(ldts), SYSDATE()
                       FROM {{ this }}
                       HAVING MAX(ldts) IS NOT NULL
{% endmacro %}