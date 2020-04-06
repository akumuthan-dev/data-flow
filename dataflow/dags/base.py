import sys

from datetime import datetime, timedelta
from functools import partial
from typing import List, Optional, Type

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from dataflow import config
from dataflow.operators.csv_outputs import create_csv
from dataflow.operators.db_tables import (
    check_table_data,
    create_temp_tables,
    drop_swap_tables,
    drop_temp_tables,
    insert_data_into_db,
    swap_dataset_tables,
)
from dataflow.utils import TableConfig, slack_alert


class PipelineMeta(type):
    """Metaclass to register pipeline DAGs for airflow discovery.

    When creating a class object also creates an instance of the class and
    saves the `.get_dag()` return value in the class module's __dict__,
    where it can be discovered by Airflow.

    To avoid registering DAGs for base classes, this skips a class if its
    name starts with `_`.

    """

    def __new__(mcls, name, bases, attrs):
        pipeline = super(PipelineMeta, mcls).__new__(mcls, name, bases, attrs)

        if not pipeline.__name__.startswith('_'):
            sys.modules[pipeline.__module__].__dict__[
                pipeline.__name__ + "__dag"
            ] = pipeline().get_dag()

        return pipeline


class _PipelineDAG(metaclass=PipelineMeta):
    target_db: str = config.DATASETS_DB_NAME
    start_date: datetime = datetime(2019, 11, 5)
    end_date: Optional[datetime] = None
    schedule_interval: str = "@daily"
    catchup: bool = False

    # Enable or disable Slack notification when DAG run finishes
    alert_on_success: bool = False
    alert_on_failure: bool = True

    # Disables the null columns check that makes sure all DB columns
    # have at least one non-null value
    allow_null_columns: bool = False

    table_config: TableConfig

    dependencies: List[Type["_PipelineDAG"]] = []

    # Controls how long will the task wait for dependencies to succeed
    dependencies_timeout: int = 8 * 3600

    # Offset between dependencies and this DAG running. So for example
    # if this pipeline starts at 5am and depends on a task that starts
    # at midnight the offset should be 5 hours. This will check for the
    # exact time, NOT a range of "between now and 5 hours ago".
    dependencies_execution_delta: timedelta = timedelta(hours=0)

    def get_fetch_operator(self) -> PythonOperator:
        raise NotImplementedError(
            f"{self.__class__} needs to override get_fetch_operator"
        )

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
                "retry_delay": timedelta(minutes=5),
                'catchup': self.catchup,
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
            max_active_runs=1,
            on_success_callback=partial(slack_alert, success=True)
            if self.alert_on_success
            else None,
            on_failure_callback=partial(slack_alert, success=False)
            if self.alert_on_failure
            else None,
        )

        _fetch = self.get_fetch_operator()
        _fetch.dag = dag

        _create_tables = PythonOperator(
            task_id="create-temp-tables",
            python_callable=create_temp_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
            dag=dag,
        )

        _insert_into_temp_table = PythonOperator(
            task_id="insert-into-temp-table",
            python_callable=insert_data_into_db,
            provide_context=True,
            op_kwargs=(dict(target_db=self.target_db, table_config=self.table_config)),
        )

        _check_tables = PythonOperator(
            task_id="check-temp-table-data",
            python_callable=check_table_data,
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
            op_kwargs={'allow_null_columns': self.allow_null_columns},
        )

        _swap_dataset_tables = PythonOperator(
            task_id="swap-dataset-table",
            python_callable=swap_dataset_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
        )

        _drop_temp_tables = PythonOperator(
            task_id="drop-temp-tables",
            python_callable=drop_temp_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            trigger_rule="one_failed",
            op_args=[self.target_db, *self.table_config.tables],
        )

        _drop_swap_tables = PythonOperator(
            task_id="drop-swap-tables",
            python_callable=drop_swap_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
        )

        (
            [_fetch, _create_tables]
            >> _insert_into_temp_table
            >> _check_tables
            >> _swap_dataset_tables
            >> _drop_swap_tables
        )

        _insert_into_temp_table >> _drop_temp_tables

        for dependency in self.dependencies:
            sensor = ExternalTaskSensor(
                task_id=f'wait-for-{dependency.__name__.lower()}',
                pool="sensors",
                external_dag_id=dependency.__name__,
                external_task_id='swap-dataset-table',
                execution_delta=self.dependencies_execution_delta,
                timeout=self.dependencies_timeout,
                dag=dag,
            )

            sensor >> [_fetch, _create_tables]

        return dag


class _CSVPipelineDAG(metaclass=PipelineMeta):
    target_db: str = config.DATASETS_DB_NAME
    start_date: datetime = datetime(2019, 11, 5)
    end_date: Optional[datetime] = None
    schedule_interval: str = "@daily"
    catchup: bool = True

    # Static pipeliens are not refreshed daily, so generated files won't be updated
    static: bool = False

    # S3 file name prefix
    base_file_name: str

    # Controls whether the current timestamp is appended to the base_file_name in S3 path
    timestamp_output: bool = True

    # DB query to generate data for the CSV file
    query: str

    dependencies: List[Type["_PipelineDAG"]] = []

    # Controls how long will the task wait for dependencies to succeed
    dependencies_timeout: int = 8 * 3600

    # Offset between dependencies and this DAG running. So for example
    # if this pipeline starts at 5am and depends on a task that starts
    # at midnight the offset should be 5 hours. This will check for the
    # exact time, NOT a range of "between now and 5 hours ago".
    dependencies_execution_delta: timedelta = timedelta(hours=0)

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2019, 1, 1),
            },
            user_defined_macros={"dependencies": self.dependencies},
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
        )

        _create_csv = PythonOperator(
            task_id=f'create-csv-current',
            python_callable=create_csv,
            provide_context=True,
            op_args=[
                self.target_db,
                self.base_file_name,
                self.timestamp_output,
                self.query,
            ],
            dag=dag,
        )

        for dependency in self.dependencies:
            sensor = ExternalTaskSensor(
                task_id=f'wait-for-{dependency.__name__.lower()}',
                pool="sensors",
                external_dag_id=dependency.__name__,
                external_task_id='swap-dataset-table',
                execution_delta=self.dependencies_execution_delta,
                timeout=self.dependencies_timeout,
                dag=dag,
            )

            sensor >> [_create_csv]

        return dag
