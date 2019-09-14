from datetime import timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import constants
from dataflow.meta import view_pipelines
from dataflow.utils import XCOMIntegratedPostgresOperator, get_defined_pipeline_classes_by_key


view_pipeline_classes = get_defined_pipeline_classes_by_key(view_pipelines, 'ViewPipeline')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
    WHERE
"""

if constants.DEBUG:
    list_all_views = """
        select table_schema as schema_name,
               table_name as view_name
        from information_schema.views
        where table_schema not in ('information_schema', 'pg_catalog')
        order by schema_name,
                 view_name;
    """


for pipeline in view_pipeline_classes:
    user_defined_macros = {
        'view_name': pipeline.view_name,
        'table_name': pipeline.dataset_pipeline.table_name,
    }
    if getattr(pipeline, 'params', None):
        user_defined_macros.update(pipeline.params)

    if pipeline.fields == '__all__':
        user_defined_macros.update({
            'fields': [(field_name, field_name) for _, field_name, _ in pipeline.dataset_pipeline.field_mapping],
        })
    else:
        user_defined_macros.update({
            'fields': pipeline.fields,
        })

    with DAG(
        pipeline.__name__,
        catchup=pipeline.catchup,
        default_args=default_args,
        start_date=pipeline.start_date,
        end_date=pipeline.end_date,
        schedule_interval=pipeline.schedule_interval,
        user_defined_macros=user_defined_macros,
    ) as dag:
        PostgresOperator(
            task_id='create-view',
            sql=create_view + pipeline.where_clause,
            postgres_conn_id=pipeline.dataset_pipeline.target_db,
        )
        if constants.DEBUG:
            XCOMIntegratedPostgresOperator(
                task_id='list-views',
                sql=list_all_views,
                postgres_conn_id=pipeline.dataset_pipeline.target_db,
            )

        globals()[pipeline.__name__] = dag
