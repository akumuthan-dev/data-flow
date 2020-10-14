from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dataflow.dags.base import PipelineMeta
from dataflow.operators.dss_generic import (
    get_table_config,
    fetch_data,
    create_temp_tables,
    insert_into_temp_table,
    swap_dataset_tables,
    drop_temp_tables,
    drop_swap_tables,
    trigger_matching,
)
from datetime import timedelta


class DSSGenericPipeline(metaclass=PipelineMeta):
    start_date = days_ago(1)
    schedule_interval = None

    def get_dag(self):

        dag = DAG(
            self.__class__.__name__,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
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

        _swap_dataset_tables = PythonOperator(
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            python_callable=swap_dataset_tables,
            task_id='swap-dataset-tables',
        )

        _drop_temp_tables = PythonOperator(
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            python_callable=drop_temp_tables,
            trigger_rule='one_failed',
            task_id='drop-temp-tables',
        )

        _drop_swap_tables = PythonOperator(
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            python_callable=drop_swap_tables,
            task_id='drop-swap-tables',
        )

        _trigger_matching = PythonOperator(
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            python_callable=trigger_matching,
            task_id='company_matching',
        )

        _get_table_config >> _create_temp_tables
        _get_table_config >> _fetch

        (
            [_create_temp_tables, _fetch]
            >> _insert_into_temp_table
            >> _swap_dataset_tables
        )

        _swap_dataset_tables >> _drop_temp_tables
        _swap_dataset_tables >> _drop_swap_tables
        _swap_dataset_tables >> _trigger_matching

        return dag
