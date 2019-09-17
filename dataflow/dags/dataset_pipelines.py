"""A module that defines Airflow DAGS for dataset pipelines."""

import json
import logging
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from jinja2 import Template

from mohawk import Sender
from mohawk.exc import HawkFail

from psycopg2 import sql

import requests

from dataflow import constants
from dataflow.meta import dataset_pipelines
from dataflow.utils import XCOMIntegratedPostgresOperator, get_defined_pipeline_classes_by_key


dataset_pipeline_classes = get_defined_pipeline_classes_by_key(dataset_pipelines, 'DatasetPipeline')

credentials = {
    'id': constants.HAWK_ID,
    'key': constants.HAWK_KEY,
    'algorithm': constants.HAWK_ALGORITHM,
}


def run_fetch(source_url, **kwargs):
    """Fetch data from source.

    Args:
        source_url (str): URL for API Endpoint to fetch data from source.

    Source endpoint has to accept GET request and respond with HTTP 200 OK for success.
    Needs to be paginated, response is expected to have below structure;
    {
        'next': <link_to_next_page>,
        'results': [list of dict]
    }

    Notes:
    XCOM isn't used to transfer data between tasks because it is not built to handle
    very large data transfer between tasks.
    Saving fetched records into a file would prevent us from scaling with celery, saving into
    a single variable causes worker shutdown due to high memory usage. That's why,
    this saves paginated response into indexed named variables and returns variable names
    to be consumed by the task which inserts data into regarding table. Alternatives are;
    - Shared network storage
    - S3 (Security risk)

    Example source_url.
        source_url = https://datahub-api-demo.london.cloudapps.digital/v4/datasets/omis-dataset
    TODO:
        By the impletation of other Datasets pipeline, there will be more generic structure to
        support various pipeline types.

    """
    result_variable_name_prefix = kwargs['run_fetch_task_id']
    variable_names = []
    index = 0
    while True:
        sender = Sender(
            credentials,
            source_url,
            'get',
            always_hash_content=False,
        )

        response = requests.get(
            source_url,
            headers={'Authorization': sender.request_header},
        )
        if response.status_code != 200:
            raise Exception(
                f'GET request to {source_url} is unsuccessful\n'
                f'Message: {response.text}',
            )
        try:
            sender.accept_response(response.headers['Server-Authorization'],
                                   content=response.content,
                                   content_type=response.headers['Content-Type'])
        except HawkFail as e:
            raise Exception(f'HAWK Authentication failed {str(e)}')

        response_json = response.json()
        if 'results' not in response_json or 'next' not in response_json:
            raise Exception('Unexpected response structure')

        Variable.set(
            f'{result_variable_name_prefix}{index}',
            response_json['results'],
            serialize_json=True,
        )
        variable_names.append(f'{result_variable_name_prefix}{index}')
        next_page = response_json['next']
        if next_page:
            index += 1
            source_url = next_page
            logging.info('Moving on to the next page')
        else:
            break

    logging.info('Fetching from source completed')
    return variable_names


def create_and_insert_into(target_db, **kwargs):
    """Insert fetched data into target database table.

    If target table doesn't exits, it creates one and inserts fetched data into it.
    If it exists, it'll try to create and insert into a copy table before running any query
    against target table.
    """
    create_table_sql = """
        CREATE TABLE "{{ table_name }}" (
        {% for _, tt_field_name, tt_field_constraints in field_mapping %}
            {{ tt_field_name }} {{ tt_field_constraints }}{{ "," if not loop.last }}
        {% endfor %}
        );
    """

    create_copy_table_sql = """
        DROP TABLE IF EXISTS "{{ table_name }}_copy";
        CREATE TABLE "{{ table_name }}_copy" as
            SELECT * from "{{ table_name }}"
        with no data;
    """

    insert_into_sql = """
        INSERT INTO "{{ table_name }}" (
        {% for _, tt_field_name, _ in field_mapping %}
            {{ tt_field_name }}{{ "," if not loop.last }}
        {% endfor %}
        )
        VALUES
        {% for record in record_subset %}
        (
            {% for st_field_name, _, _ in field_mapping %}
                {% if not record[st_field_name] or record[st_field_name] == 'None' %}
                    NULL
                {% else %}
                    {{ record[st_field_name] }}
                {% endif %}
                {{ "," if not loop.last }}
            {% endfor %}
        {{ ")," if not loop.last }}
        {% endfor %}
        );
    """

    def execute_insert_into(table_name):

        for var_name in var_names:
            record_subset = json.loads(Variable.get(var_name))
            escaped_record_subset = []
            for record in record_subset:
                escaped_record = {}
                for key, value in record.items():
                    if value and value != 'None':
                        escaped_record[key] = sql.Literal(value).as_string(target_db_conn)
                    else:
                        escaped_record[key] = sql.Literal(None).as_string(target_db_conn)
                escaped_record_subset.append(escaped_record)

            exec_sql = Template(insert_into_sql).render(
                table_name=table_name,
                field_mapping=field_mapping,
                record_subset=escaped_record_subset,
            )
            target_db_cursor.execute(exec_sql)

        target_db_conn.commit()

    table_name = kwargs['table_name']
    field_mapping = kwargs['field_mapping']
    run_fetch_task_id = kwargs['run_fetch_task_id']
    task_instance = kwargs['task_instance']

    # Get name of variables which hold paginated response result
    var_names = task_instance.xcom_pull(task_ids=run_fetch_task_id)
    table_exists = task_instance.xcom_pull(task_ids='check-if-table-exists')[0][0]
    try:
        target_db_conn = PostgresHook(postgres_conn_id=target_db).get_conn()
        target_db_cursor = target_db_conn.cursor()

        # If table already exists in the target database, try inserting fetched data into
        # a copy table to protect target table. If succeeds, delete all data from target table
        # until there will be possibility for incremental load
        if table_exists and table_exists != 'None':
            rendered_create_copy_table_sql = Template(create_copy_table_sql).render(
                table_name=table_name,
                field_mapping=field_mapping,
            )
            target_db_cursor.execute(rendered_create_copy_table_sql)

            execute_insert_into(f'{table_name}_copy')
            target_db_cursor.execute(f'DELETE FROM {table_name};')

        else:
            rendered_create_table_sql = Template(create_table_sql).render(
                table_name=table_name,
                field_mapping=field_mapping,
            )
            target_db_cursor.execute(rendered_create_table_sql)

        execute_insert_into(table_name)

    # TODO: Gotta Catch'm all
    except Exception as e:
        logging.error(f'Exception: {e}')
        target_db_conn.rollback()
        raise

    finally:
        if target_db_conn:
            target_db_cursor.close()
            target_db_conn.close()


def clean_result_variables(**kwargs):
    """Delete all airflow variables generated by this pipeline."""
    var_names = kwargs['task_instance'].xcom_pull(task_ids=kwargs['run_fetch_task_id'])
    for var_name in var_names:
        Variable.delete(var_name)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

check_if_table_exists = "SELECT to_regclass('{{ table_name }}');"

select_from_target_table = 'SELECT * FROM "%s";'

for pipeline in dataset_pipeline_classes:
    run_fetch_task_id = f'RunFetch{pipeline.__name__}'

    with DAG(
        pipeline.__name__,
        catchup=False,
        default_args=default_args,
        start_date=pipeline.start_date,
        end_date=pipeline.end_date,
        schedule_interval=pipeline.schedule_interval,
        user_defined_macros={
            'table_name': pipeline.table_name,
            'field_mapping': pipeline.field_mapping,
            'run_fetch_task_id': run_fetch_task_id,
        },
    ) as dag:
        t1 = PythonOperator(
            task_id=run_fetch_task_id,
            python_callable=run_fetch,
            provide_context=True,
            op_args=[f'{pipeline.source_url}'],
        )

        t2 = XCOMIntegratedPostgresOperator(
            task_id='check-if-table-exists',
            sql=check_if_table_exists,
            postgres_conn_id=pipeline.target_db,
        )

        t3 = PythonOperator(
            task_id='create-and-insert',
            python_callable=create_and_insert_into,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )
        t4 = PythonOperator(
            task_id='clean-result-variables',
            python_callable=clean_result_variables,
            provide_context=True,
        )

        t4 << t3 << [t1, t2]
        globals()[pipeline.__name__] = dag
