from urllib.parse import quote

import sqlalchemy as sa
from airflow.models import DagBag
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import dataflow.operators.db_tables
from dataflow.config import (
    DATASETS_DB_NAME,
    DATA_STORE_SERVICE_BASE_URL,
    DATA_STORE_SERVICE_HAWK_CREDENTIALS,
)
from dataflow.operators.common import _hawk_api_request, fetch_from_hawk_api
from dataflow.utils import TableConfig


def create_temp_tables(*args, **context):
    table_config = context['task_instance'].xcom_pull(task_ids='get-table-config')
    return dataflow.operators.db_tables.create_temp_tables(
        DATASETS_DB_NAME,
        *table_config.tables,
        *args,
        **context,
    )


def drop_swap_tables(*args, **context):
    table_config = context['task_instance'].xcom_pull(task_ids='get-table-config')
    return dataflow.operators.db_tables.drop_swap_tables(
        DATASETS_DB_NAME,
        *table_config.tables,
        *args,
        **context,
    )


def drop_temp_tables(*args, **context):
    table_config = context['task_instance'].xcom_pull(task_ids='get-table-config')
    if table_config:
        return dataflow.operators.db_tables.drop_temp_tables(
            DATASETS_DB_NAME,
            *table_config.tables,
            *args,
            **context,
        )


def fetch_data(*args, **context):
    schema_name, table_name = _get_schema_and_table(context)
    source_url = (
        f'{DATA_STORE_SERVICE_BASE_URL}/api/v1/table-data/{schema_name}/{table_name}'
        f'?orientation=records'
    )
    table_config = context['task_instance'].xcom_pull(task_ids='get-table-config')
    return fetch_from_hawk_api(
        table_name=table_config.table_name,
        source_url=source_url,
        hawk_credentials=DATA_STORE_SERVICE_HAWK_CREDENTIALS,
        *args,
        **context,
    )


def get_table_config(**context):
    def to_sql_alachemy_type(column_name, data_type):
        mapping = {
            'TEXT': sa.Text,
            'INTEGER': sa.Integer,
            'TIMESTAMP WITHOUT TIME ZONE': sa.DateTime,
            'DATE': sa.Date,
            'FLOAT': sa.Float,
            'BOOLEAN': sa.Boolean,
            'BIGINT': sa.BigInteger,
            'NUMERIC': sa.Numeric,
        }
        try:
            if data_type.endswith('[]'):
                sa_data_type = sa.ARRAY(mapping[data_type[:-2]])
            else:
                sa_data_type = mapping[data_type]
        except KeyError:
            raise ValueError(f'data type {data_type} not supported')
        return sa.Column(column_name, sa_data_type)

    schema_name, table_name = _get_schema_and_table(context)
    source_url = f'{DATA_STORE_SERVICE_BASE_URL}/api/v1/table-structure/{quote(schema_name)}/{quote(table_name)}'

    # transform schema and table in line with dataflow conventions
    target_schema_name, target_table_name = _parse_schema(schema_name)

    table_fields = _hawk_api_request(
        url=source_url,
        credentials=DATA_STORE_SERVICE_HAWK_CREDENTIALS,
        results_key='columns',
        next_key=None,
    )['columns']
    table_fields = [
        (field['name'], to_sql_alachemy_type(field['name'], field['type']))
        for field in table_fields
    ]
    table_config = TableConfig(
        schema=target_schema_name,
        table_name=target_table_name,
        field_mapping=table_fields,
    )
    return table_config


def insert_into_temp_table(*args, **context):
    table_config = context['task_instance'].xcom_pull(task_ids='get-table-config')
    return dataflow.operators.db_tables.insert_data_into_db(
        target_db=DATASETS_DB_NAME,
        table_config=table_config,
        *args,
        **context,
    )


def swap_dataset_tables(*args, **context):
    table_config = context['task_instance'].xcom_pull(task_ids='get-table-config')
    return dataflow.operators.db_tables.swap_dataset_tables(
        DATASETS_DB_NAME,
        *table_config.tables,
        *args,
        **context,
    )


def trigger_matching(**context):
    schema_name, _ = _get_schema_and_table(context)
    target_schema_name, target_table_name = _parse_schema(schema_name)
    target_tag = f'DSSGenericMatchingPipeline-{target_schema_name}-{target_table_name}'
    target_dag = None
    for dag in DagBag().dags.values():
        if dag.tags and target_tag in dag.tags:
            target_dag = dag
            break
    if target_dag:
        TriggerDagRunOperator(
            trigger_dag_id=target_dag.dag_id, task_id='trigger-match'
        ).execute(context)


def _get_schema_and_table(context):
    config = context['dag_run'].conf
    if not config:
        raise ValueError("no config provided")

    table_name = config.get('data_uploader_table_name', None)
    schema_name = config.get('data_uploader_schema_name', None)

    if not table_name or not schema_name:
        raise ValueError("no table or schema provided in config")

    return schema_name, table_name


def _parse_schema(schema_name):
    # transform schema and table in line with dataflow conventions
    schema_parts = schema_name.split('.')
    if len(schema_parts) != 2:
        raise ValueError(
            'invalid schema name: "organisation"."dataset" structure expected'
        )
    organisation = schema_parts[0]
    dataset = schema_parts[1]

    target_schema_name = organisation.lower()
    target_table_name = dataset.lower()
    return target_schema_name, target_table_name
