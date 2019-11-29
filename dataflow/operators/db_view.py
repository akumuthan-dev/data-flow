create_view = """
    CREATE {% if materialized %}MATERIALIZED{% else %}OR REPLACE{%endif %} VIEW
        {{ view_name }}_{{ (
            macros.datetime.strptime(ds, '%Y-%m-%d') +
            macros.dateutil.relativedelta.relativedelta(months=+1, days=-1)
        ).date() | replace('-', '_') }} AS
"""

list_all_views = """
    select table_schema as schema_name, table_name as view_name
    from information_schema.views
    where table_schema not in ('information_schema', 'pg_catalog')
    order by schema_name, view_name;
"""
