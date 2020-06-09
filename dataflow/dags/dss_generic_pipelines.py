import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dataflow.dags.base import PipelineMeta
from dataflow.operators.dss_generic import (
    get_table_config,
    fetch_data,
    create_temp_tables,
    insert_into_temp_table,
    check_tables,
    swap_dataset_tables,
    drop_temp_tables,
    drop_swap_tables,
)


class DSSGenericPipeline(metaclass=PipelineMeta):
    def get_dag(self):

        dag = DAG(
            self.__class__.__name__, schedule_interval=None, start_date=days_ago(1),
        )

        _get_table_config = PythonOperator(
            provide_context=True,
            dag=dag,
            python_callable=get_table_config,
            task_id='get-table-config',
        )

        _fetch = PythonOperator(
            provide_context=True,
            python_callable=fetch_data,
            retries=0,
            task_id='run-fetch',
        )

        _create_temp_tables = PythonOperator(
            provide_context=True,
            python_callable=create_temp_tables,
            task_id='create-temp-tables',
        )

        _insert_into_temp_table = PythonOperator(
            provide_context=True,
            python_callable=insert_into_temp_table,
            task_id='insert-into-temp-table',
        )

        _check_tables = PythonOperator(
            provide_context=True, python_callable=check_tables, task_id='check-tables'
        )

        _swap_dataset_tables = PythonOperator(
            execution_timeout=datetime.timedelta(minutes=10),
            provide_context=True,
            python_callable=swap_dataset_tables,
            task_id='swap-dataset-tables',
        )

        _drop_temp_tables = PythonOperator(
            execution_timeout=datetime.timedelta(minutes=10),
            provide_context=True,
            python_callable=drop_temp_tables,
            trigger_rule='one_failed',
            task_id='drop-temp-tables',
        )

        _drop_swap_tables = PythonOperator(
            execution_timeout=datetime.timedelta(minutes=10),
            provide_context=True,
            python_callable=drop_swap_tables,
            task_id='drop-swap-tables',
        )

        _get_table_config >> _create_temp_tables
        _get_table_config >> _fetch

        (
            [_create_temp_tables, _fetch]
            >> _insert_into_temp_table
            >> _check_tables
            >> _swap_dataset_tables
        )

        _swap_dataset_tables >> _drop_temp_tables
        _swap_dataset_tables >> _drop_swap_tables

        return dag
