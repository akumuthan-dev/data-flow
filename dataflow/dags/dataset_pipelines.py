"""A module that defines Airflow DAGS for dataset pipelines."""

import json
import logging
import time
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from jinja2 import Template

from mohawk import Sender
from mohawk.exc import HawkFail

from psycopg2 import sql

import redis
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


def get_redis_client():
    return redis.from_url(url=constants.REDIS_URL)


def run_fetch(source_url, run_fetch_task_id=None, task_instance=None, **kwargs):
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

    def rollback_variables(index):
        for i in range(index):
            key = f'{run_fetch_task_id}{i}'
            Variable.delete(key)
            redis_client.delete(key)

    redis_client = get_redis_client()
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
            task_instance.xcom_push(key='state', value=False)
            rollback_variables(index)
            raise Exception(
                f'GET request to {source_url} is unsuccessful\n'
                f'Message: {response.text}',
            )
        try:
            sender.accept_response(response.headers['Server-Authorization'],
                                   content=response.content,
                                   content_type=response.headers['Content-Type'])
        except HawkFail as e:
            task_instance.xcom_push(key='state', value=False)
            rollback_variables(index)
            raise Exception(f'HAWK Authentication failed {str(e)}')

        response_json = response.json()
        if 'results' not in response_json or 'next' not in response_json:
            task_instance.xcom_push(key='state', value=False)
            rollback_variables(index)
            raise Exception('Unexpected response structure')

        key = f'{run_fetch_task_id}{index}'
        Variable.set(
            key,
            response_json['results'],
            serialize_json=True,
        )
        redis_client.set(key, 1)
        next_page = response_json['next']
        if next_page:
            index += 1
            source_url = next_page
            logging.info('Moving on to the next page')
        else:
            break

    logging.info('Fetching from source completed')
    task_instance.xcom_push(key='state', value=True)


def create_or_delete_from_target_table(
    target_db,
    table_name=None,
    field_mapping=None,
    task_instance=None,
    **kwargs,
):
    """Create target database table or delete all from it.

    If target table exists, this'll delete all from target table.
    If target table doesn't exits, this creates one.
    """
    create_table_sql = """
        CREATE TABLE {{ table_name }} (
        {% for _, tt_field_name, tt_field_constraints in field_mapping %}
            {{ tt_field_name }} {{ tt_field_constraints }}{{ "," if not loop.last }}
        {% endfor %}
        );
    """

    table_exists = task_instance.xcom_pull(task_ids='check-if-table-exists')[0][0]
    try:
        target_db_conn = PostgresHook(postgres_conn_id=target_db).get_conn()
        target_db_cursor = target_db_conn.cursor()

        # If table already exists in the target database, delete all from target table. If not exists, create one.
        # Until there will be possibility for incremental load
        if table_exists and table_exists != 'None':
            target_db_cursor.execute('DELETE FROM {0};'.format(sql.Identifier(table_name).as_string(target_db_conn)))
            logging.info('Deleting from target table')
        else:
            logging.info('Creating a target table')
            rendered_create_table_sql = Template(create_table_sql).render(
                table_name=sql.Identifier(table_name).as_string(target_db_conn),
                field_mapping=field_mapping,
            )
            target_db_cursor.execute(rendered_create_table_sql)

        target_db_conn.commit()

    # TODO: Gotta Catch'm all
    except Exception as e:
        logging.error(f'Exception: {e}')
        target_db_conn.rollback()
        raise

    finally:
        if target_db_conn:
            target_db_cursor.close()
            target_db_conn.close()


def get_available_page_var(redis_client, pattern):
    for key in redis_client.keys(pattern=f'{pattern}*'):
        logging.info(f'Getting available page {key}')
        redis_value = redis_client.get(key)
        logging.info(f'redis value {redis_value}')
        if redis_value:
            logging.info(f'Found an unprocessed one {key}')
            result = redis_client.delete(key)
            if result == 0:
                logging.info(f'Another worker already got this {key}')
                continue

            return key


def execute_insert_into(
    target_db,
    table_name=None,
    run_fetch_task_id=None,
    field_mapping=None,
    task_instance=None,
    **kwargs
):

    insert_into_sql = """
        INSERT INTO {{ table_name }} (
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
    # Give some initial time to fetch task to get a page and save it into variable
    sleep_time = 5
    runs = 1
    time.sleep(5)
    redis_client = get_redis_client()
    try:
        target_db_conn = PostgresHook(postgres_conn_id=target_db).get_conn()
        target_db_cursor = target_db_conn.cursor()

        while True:
            var_name = get_available_page_var(redis_client, run_fetch_task_id)
            logging.info(f'Got the var_name {var_name}')
            if var_name:
                sleep_time = 5
                var_name = var_name.decode('utf-8')
                try:
                    record_subset = json.loads(Variable.get(var_name))
                except KeyError as e:
                    logging.info(f'Var {var_name} no more exist')
                    continue

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
                    table_name=sql.Identifier(table_name).as_string(target_db_conn),
                    field_mapping=field_mapping,
                    record_subset=escaped_record_subset,
                )
                target_db_cursor.execute(exec_sql)
                logging.info(f'Deleting the var_name {var_name}')
                Variable.delete(var_name)
            else:
                # Check if fetch task completed successfully, if it's, break out of loop and commit
                # the transaction because there is no more page to process. If it's failed raise Exception so that
                # transaction will be rollbacked
                state = task_instance.xcom_pull(key='state', task_ids=run_fetch_task_id)
                logging.info(f'Checking state {state}')
                logging.info('state type {}'.format(type(state)))
                if state is False:
                    raise Exception('Fetching task failed!')
                elif state is True:
                    logging.info(f'state is True')
                    break
                else:
                    logging.info(f'Sleeping for {sleep_time} fetch task to catchup')
                    sleep_time = sleep_time * runs
                    time.sleep(sleep_time)
                    runs += 1

        target_db_conn.commit()

    # TODO: Gotta Catch'm all
    except Exception as e:
        logging.error(f'Exception: {e}')
        target_db_conn.rollback()
        raise

    finally:
        if target_db_conn:
            target_db_cursor.close()
            target_db_conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
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
            task_id='create-or-delete-from-target-table',
            python_callable=create_or_delete_from_target_table,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        insert_group = []
        for index in range(constants.NUMBER_OF_WORKER):
            insert_group.append(
                PythonOperator(
                    task_id=f'execute-insert-into-{index}',
                    python_callable=execute_insert_into,
                    provide_context=True,
                    op_args=[f'{pipeline.target_db}'],
                )
            )

        insert_group << t3 << t2
        globals()[pipeline.__name__] = dag
