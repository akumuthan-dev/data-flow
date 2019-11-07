create_view = """
    DROP VIEW IF EXISTS
        {{ view_name }}_{{ (
            macros.datetime.strptime(ds, '%Y-%m-%d') +
            macros.dateutil.relativedelta.relativedelta(months=+1, days=-1)
        ).date() | replace('-', '_') }};

    CREATE VIEW
        {{ view_name }}_{{ (
            macros.datetime.strptime(ds, '%Y-%m-%d') +
            macros.dateutil.relativedelta.relativedelta(months=+1, days=-1)
        ).date() | replace('-', '_') }}

    AS SELECT
    {% for field_name, field_alias in fields %}
        {{ field_name }} AS "{{ field_alias }}"{{ "," if not loop.last }}
    {% endfor %}
    FROM "{{ table_name }}"
    {{ join_clause }}
    WHERE
"""

list_all_views = """
    select table_schema as schema_name,
            table_name as view_name
    from information_schema.views
    where table_schema not in ('information_schema', 'pg_catalog')
    order by schema_name,
                view_name;
"""
