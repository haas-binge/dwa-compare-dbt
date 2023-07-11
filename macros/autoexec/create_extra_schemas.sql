{% macro create_extra_schemas() %}

{% do log('create schema if not exists ' ~ var("meta_schema"), True) %}
create schema if not exists {{ var("meta_schema") }};

{% do log('create schema if not exists dwh_01_ext', True) %}
create schema if not exists dws.dwh_01_ext;

{% endmacro %}