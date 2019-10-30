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
from dataflow.utils import get_defined_pipeline_classes_by_key


dataset_pipeline_classes = get_defined_pipeline_classes_by_key(dataset_pipelines, 'DatasetPipeline')

credentials = {
    'id': constants.HAWK_ID,
    'key': constants.HAWK_KEY,
    'algorithm': constants.HAWK_ALGORITHM,
}


def get_redis_client():
    """Returns redis client from connection URL"""
    return redis.from_url(url=constants.REDIS_URL)


def mark_task_failed(task_instance):
    """Marks task as failed and delete variables set by the task"""
    redis_client = get_redis_client()
    task_instance.xcom_push(key='state', value=False)
    for i in range(0, redis_client.llen(run_fetch_task_id)):
        Variable.delete(redis_client.lindex(run_fetch_task_id, i))
    redis_client.delete(run_fetch_task_id)


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
    redis_client = get_redis_client()
    # Clear any leftover requests from previous task runs
    redis_client.delete(run_fetch_task_id)

    index = 0
    while True:
        sender = Sender(
            credentials,
            source_url,
            'get',
            always_hash_content=False,
        )

        logging.info(f'Fetching page {source_url}')
        response = requests.get(
            source_url,
            headers={'Authorization': sender.request_header},
        )
        if response.status_code != 200:
            mark_task_failed(task_instance)
            raise Exception(
                f'GET request to {source_url} is unsuccessful\n'
                f'Message: {response.text}',
            )
        try:
            sender.accept_response(response.headers['Server-Authorization'],
                                   content=response.content,
                                   content_type=response.headers['Content-Type'])
        except HawkFail as e:
            mark_task_failed(task_instance)
            raise Exception(f'HAWK Authentication failed {str(e)}')

        response_json = response.json()
        if 'results' not in response_json or 'next' not in response_json:
            mark_task_failed(task_instance)
            raise Exception('Unexpected response structure')

        key = f'{run_fetch_task_id}{index}'
        Variable.set(key, response_json['results'], serialize_json=True)
        redis_client.rpush(run_fetch_task_id, key)
        next_page = response_json['next']
        if next_page:
            index += 1
            source_url = next_page
        else:
            break

    logging.info('Fetching from source completed')
    task_instance.xcom_push(key='state', value=True)


def create_tables(
    target_db,
    table_name=None,
    field_mapping=None,
    **kwargs,
):
    """
    Create a temporary table to be copied over to the target table `table_name`.
    """
    temp_table_name = F'{table_name}_tmp'
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS {{ table_name }} (
        {% for _, tt_field_name, tt_field_constraints in field_mapping %}
            {{ tt_field_name }} {{ tt_field_constraints }}{{ "," if not loop.last }}
        {% endfor %}
        );
    """

    try:
        target_db_conn = PostgresHook(postgres_conn_id=target_db).get_conn()
        target_db_cursor = target_db_conn.cursor()
        logging.info(f'Creating temporary table {temp_table_name}')
        target_db_cursor.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(
            sql.Identifier(temp_table_name)
        ))
        target_db_cursor.execute(
            Template(create_table_sql).render(
                table_name=sql.Identifier(temp_table_name).as_string(target_db_conn),
                field_mapping=field_mapping,
            )
        )
        logging.info(f'Creating target table {table_name}')
        target_db_cursor.execute(
            Template(create_table_sql).render(
                table_name=sql.Identifier(table_name).as_string(target_db_conn),
                field_mapping=field_mapping,
            )
        )
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


def insert_from_temporary_table(
    target_db,
    table_name=None,
    task_instance=None,
    run_fetch_task_id=None,
    **kwargs
):
    """
    Insert data from temporary table into `table_name`.
    The rational behind is not doing any modification on target table
    before we make sure fetching from source and insertion is successful.
    """
    temp_table_name = F'{table_name}_tmp'
    rename_table_sql = """
        TRUNCATE TABLE {table_name};
        INSERT INTO {table_name}
        SELECT * FROM {temp_table_name};
        DROP TABLE {temp_table_name};
    """
    fetcher_state = task_instance.xcom_pull(key='state', task_ids=run_fetch_task_id)
    inserter_state = True
    # Check all insertion tasks are completed successfully.
    for index in range(constants.INGEST_TASK_CONCURRENCY):
        inserter_state = (
            inserter_state and task_instance.xcom_pull(
                key='state',
                task_ids=f'execute-insert-into-{index}',
            )
        )
    if fetcher_state is True and inserter_state is True:
        logging.info(f'Inserting data from {temp_table_name} into {table_name}')
        try:
            target_db_conn = PostgresHook(postgres_conn_id=target_db).get_conn()
            target_db_cursor = target_db_conn.cursor()
            target_db_cursor.execute(
                rename_table_sql.format(
                    table_name=sql.Identifier(table_name).as_string(target_db_conn),
                    temp_table_name=sql.Identifier(temp_table_name).as_string(target_db_conn),
                ),
            )
            target_db_conn.commit()

        # TODO: Gotta Catch'm all
        except Exception as e:
            logging.error(f'Exception: {e}')
            mark_task_failed(task_instance)
            target_db_conn.rollback()
            raise

        finally:
            if target_db_conn:
                target_db_cursor.close()
                target_db_conn.close()
    else:
        logging.info('Fetcher or inserter failed!')
        mark_task_failed(task_instance)


def execute_insert_into(
    target_db,
    table_name=None,
    run_fetch_task_id=None,
    field_mapping=None,
    task_instance=None,
    **kwargs
):
    """Inserts each paginated response data into target database table.
    Polls to find variable hasn't been processed, generates regarding sql statement to
    insert data in, incrementally waits for new variables.
    Success depends on fetcher task completion.
    """
    temp_table_name = F'{table_name}_tmp'
    insert_into_sql = """
        INSERT INTO {{ temp_table_name }} (
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
    time.sleep(3)
    # Used for providing incremental wait
    sleep_time = 5
    number_of_runs = 1
    redis_client = get_redis_client()
    try:
        target_db_conn = PostgresHook(postgres_conn_id=target_db).get_conn()
        target_db_cursor = target_db_conn.cursor()
        while True:
            var_name = redis_client.lpop(run_fetch_task_id)
            if var_name:
                logging.info(f'Processing page {var_name}')
                sleep_time = 5
                var_name = var_name.decode('utf-8')
                try:
                    record_subset = json.loads(Variable.get(var_name))
                except KeyError:
                    logging.info(f'Page {var_name} does not exist. Moving on.')
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
                    temp_table_name=sql.Identifier(temp_table_name).as_string(target_db_conn),
                    field_mapping=field_mapping,
                    record_subset=escaped_record_subset,
                )
                target_db_cursor.execute(exec_sql)
                logging.info(f'Page {var_name} ingested successfully')
                Variable.delete(var_name)
            else:
                # Check if fetch task completed successfully, if it's, break out of loop and commit
                # the transaction because there is no more page to process.
                # If it's failed raise Exception so that transaction will be rollbacked
                state = task_instance.xcom_pull(key='state', task_ids=run_fetch_task_id)
                logging.info(f'Fetcher task {run_fetch_task_id} state is "{state}"')
                if state is False:
                    mark_task_failed(task_instance)
                    raise Exception(f'Fetcher task {run_fetch_task_id} failed!')
                elif state is True:
                    logging.info(f'Fetcher task {run_fetch_task_id} has completed.')
                    break
                else:
                    logging.info(
                        f'Sleeping for {sleep_time} so task {run_fetch_task_id} can catchup'
                    )
                    sleep_time = sleep_time * number_of_runs
                    time.sleep(sleep_time)
                    number_of_runs += 1

        target_db_conn.commit()
        task_instance.xcom_push(key='state', value=True)

    # TODO: Gotta Catch'm all
    except Exception as e:
        logging.error(f'Exception: {e}')
        mark_task_failed(task_instance)
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

for pipeline in dataset_pipeline_classes:
    run_fetch_task_id = f'RunFetch{pipeline.__name__}'

    with DAG(
        pipeline.__name__,
        catchup=False,
        default_args=default_args,
        start_date=pipeline.start_date,
        end_date=pipeline.end_date,
        schedule_interval=pipeline.schedule_interval,
        max_active_runs=1,
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

        t3 = PythonOperator(
            task_id='create-tables',
            python_callable=create_tables,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        insert_group = []
        for index in range(constants.INGEST_TASK_CONCURRENCY):
            insert_group.append(
                PythonOperator(
                    task_id=f'execute-insert-into-{index}',
                    python_callable=execute_insert_into,
                    provide_context=True,
                    op_args=[f'{pipeline.target_db}'],
                )
            )

        tend = PythonOperator(
            task_id='insert-from-temporary-table',
            python_callable=insert_from_temporary_table,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        tend << insert_group << t3
        globals()[pipeline.__name__] = dag
