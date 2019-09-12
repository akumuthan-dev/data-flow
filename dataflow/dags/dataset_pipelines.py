import os

from datetime import timedelta

import requests

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from mohawk import Sender
from mohawk.exc import HawkFail

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

    Example source_url.
        source_url = https://datahub-api-demo.london.cloudapps.digital/v4/datasets/omis-dataset
    TODO:
        By the impletation of other Datasets pipeline, there will be more generic structure to
        support various pipeline types.
    """
    results = []
    while True:
        sender = Sender(
            credentials,
            source_url,
            'get',
            always_hash_content=False
        )

        response = requests.get(
            source_url,
            headers={'Authorization': sender.request_header}
        )
        if response.status_code != 200:
            raise Exception(
                f'GET request to {source_url} is unsuccessful\n'
                f'Message: {response.text}'
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

        results += response_json['results']
        next_page = response_json['next']
        if next_page:
            source_url = next_page
            print('Moving on to the next page')
        else:
            break

    print('Fetching from source completed')
    return results


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

check_if_table_exists = """
    SELECT to_regclass('{{ table_name }}');
"""

delete_all_and_insert = """
    {% if task_instance.xcom_pull(task_ids="check-if-table-exists")[0][0] and
        task_instance.xcom_pull(task_ids="check-if-table-exists")[0][0] != 'None' %}

        CREATE TABLE "{{ table_name }}_copy" as
            SELECT * from "{{ table_name }}"
        with no data;
        INSERT INTO "{{ table_name }}_copy" (
        {% for _, tt_field_name, _ in field_mapping %}
            {{ tt_field_name }}{{ "," if not loop.last }}
        {% endfor %}
        )
        VALUES
        {% for record in task_instance.xcom_pull(task_ids="run-dataset-pipeline")  %}
        (
            {% for st_field_name, _, _ in field_mapping %}
                {% if not record[st_field_name] or record[st_field_name] == 'None' %}
                    NULL
                {% else %}
                    {{ record[st_field_name] | replace('"', "'") | tojson | replace('"', "'") }}
                {% endif %}
                {{ "," if not loop.last }}
            {% endfor %}
        {{ ")," if not loop.last }}
        {% endfor %}
        );
        DROP TABLE "{{ table_name }}_copy";
        DELETE FROM "{{ table_name }}";
    {% else %}
        CREATE TABLE "{{ table_name }}" (
        {% for _, tt_field_name, tt_field_constraints in field_mapping %}
            {{ tt_field_name }} {{ tt_field_constraints }}{{ "," if not loop.last }}
        {% endfor %}
        );
    {% endif %}

    INSERT INTO "{{ table_name }}" (
    {% for _, tt_field_name, _ in field_mapping %}
        {{ tt_field_name }}{{ "," if not loop.last }}
    {% endfor %}
    )
    VALUES
    {% for record in task_instance.xcom_pull(task_ids="run-dataset-pipeline")  %}
    (
        {% for st_field_name, _, _ in field_mapping %}
            {% if not record[st_field_name] or record[st_field_name] == 'None' %}
                NULL
            {% else %}
                {{ record[st_field_name] | replace('"', "'") | tojson | replace('"', "'") }}
            {% endif %}
            {{ "," if not loop.last }}
        {% endfor %}
    {{ ")," if not loop.last }}
    {% endfor %}
    );
"""


for pipeline in dataset_pipeline_classes:
    with DAG(
        pipeline.__name__,
        catchup=False,
        default_args=default_args,
        start_date=pipeline.start_date,
        end_date=pipeline.end_date,
        schedule_interval=pipeline.schedule_interval,
        user_defined_macros={
            'table_name': pipeline.table_name,
            'field_mapping': pipeline.field_mapping
        }
    ) as dag:
        t1 = PythonOperator(
            task_id='run-dataset-pipeline',
            python_callable=run_fetch,
            provide_context=True,
            op_args=[f'{pipeline.source_url}'],
        )

        t2 = XCOMIntegratedPostgresOperator(
            task_id='check-if-table-exists',
            sql=check_if_table_exists,
            postgres_conn_id=pipeline.target_db
        )

        t3 = PostgresOperator(
            task_id='delete-all-and-insert',
            sql=delete_all_and_insert,
            postgres_conn_id=pipeline.target_db,
        )
        t3 << [t1, t2]
        globals()[pipeline.__name__] = dag
