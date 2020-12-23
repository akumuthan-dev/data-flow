import sys

from datetime import datetime, timedelta, time
from functools import partial
from typing import List, Optional, Type, Callable, Tuple

from airflow import DAG
from airflow.models import SkipMixin
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.utils.helpers import chain

from dataflow import config
from dataflow.operators.csv_outputs import create_csv, create_compressed_csv
from dataflow.operators.db_tables import (
    check_table_data,
    create_temp_tables,
    drop_swap_tables,
    drop_temp_tables,
    insert_data_into_db,
    swap_dataset_tables,
    branch_on_modified_date,
    scrape_load_and_check_data,
    poll_for_new_data,
    create_temp_table_indexes,
)
from dataflow.operators.email import send_dataset_update_emails
from dataflow.utils import TableConfig, slack_alert, SingleTableConfig


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
    schedule_interval: Optional[str] = "@daily"
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

    # If True, the source_data_modified_utc metadata field will be set to the swap table task run utc.
    use_utc_now_as_source_modified: bool = False

    # How many times the fetch operator should be retried if it fails
    # Needs to be added to each fetch operator implementation.
    fetch_retries = 3

    def get_fetch_operator(self) -> PythonOperator:
        raise NotImplementedError(
            f"{self.__class__} needs to override get_fetch_operator"
        )

    def get_insert_data_callable(self):
        return insert_data_into_db

    def get_source_data_modified_utc(self) -> Optional[datetime]:
        return None

    def get_source_data_modified_utc_callable(self) -> Optional[Callable]:
        return None

    def branch_on_modified_date(
        self, dag: DAG, target_db: str, table_config: TableConfig
    ) -> PythonOperator:
        """Check whether data is newer than the previous run, else abort.

        This task is executed if the DAG returns non-None from `get_source_data_modified_utc_callable` method. That
        method should return a callable which itself returns a UTC timestamp that is compared gainst the
        `dataflow.metadata` table for this pipeline. If the date is newer than our stored date, the pipeline will
        continue, otherwise we'll abort.
        """
        return BranchPythonOperator(
            task_id="branch-on-modified-date",
            python_callable=branch_on_modified_date,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[target_db, table_config],
            dag=dag,
        )

    def get_transform_operator(self):
        """
        Optional overridable task to transform/manipulate data
        between insert-into-temp-table task and check-temp-table-data task
        """
        return None

    @classmethod
    def fq_table_name(cls):
        return f'"{cls.table_config.schema}"."{cls.table_config.table_name}"'

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 3,
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

        # If we've configured a way for the pipeline to determine when the source data was last modified,
        # then we should run checks to see if the data has been updated. If it hasn't, we will end the pipeline
        # early (but gracefully) without pulling the data again.
        _get_source_data_modified_utc_callable = (
            self.get_source_data_modified_utc_callable()
        )
        if _get_source_data_modified_utc_callable:
            _get_source_modified_date_utc = PythonOperator(
                task_id="get-source-modified-date",
                python_callable=_get_source_data_modified_utc_callable,
                dag=dag,
            )
            _branch_on_modified_date = self.branch_on_modified_date(
                dag, self.target_db, self.table_config
            )

            _stop = DummyOperator(task_id="stop", dag=dag)
            _continue = DummyOperator(task_id="continue", dag=dag)
        else:
            _branch_on_modified_date = None
            _get_source_modified_date_utc = None
            _stop = None
            _continue = None

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
            python_callable=self.get_insert_data_callable(),
            provide_context=True,
            op_kwargs=(dict(target_db=self.target_db, table_config=self.table_config)),
        )

        _transform_tables = self.get_transform_operator()
        if _transform_tables:
            _transform_tables.dag = dag

        if self.table_config.indexes is not None:
            _create_post_insert_indexes = PythonOperator(
                task_id='create-post-insert-indexes',
                python_callable=create_temp_table_indexes,
                provide_context=True,
                op_args=[self.target_db, self.table_config],
            )
        else:
            _create_post_insert_indexes = None

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
            op_kwargs={
                'use_utc_now_as_source_modified': self.use_utc_now_as_source_modified
            },
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

        if _get_source_modified_date_utc:
            _get_source_modified_date_utc >> _branch_on_modified_date >> [
                _stop,
                _continue,
            ]
            _continue >> [_fetch, _create_tables]

        [_fetch, _create_tables] >> _insert_into_temp_table >> _drop_temp_tables

        _next_chain = [_insert_into_temp_table]
        if _transform_tables:
            _next_chain.append(_transform_tables)
        if _create_post_insert_indexes:
            _next_chain.append(_create_post_insert_indexes)
        _next_chain.append(_check_tables)

        chain(*_next_chain)
        del _next_chain

        _check_tables >> _swap_dataset_tables >> _drop_swap_tables

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

    # Should the output CSV be compressed (zip archive) before uploading to S3.
    compress: bool = False

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
                'retries': 3,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2019, 1, 1),
            },
            user_defined_macros={"dependencies": self.dependencies},
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
        )

        _create_csv = PythonOperator(
            task_id='create-csv-current',
            python_callable=create_compressed_csv if self.compress else create_csv,
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


class _PandasPipelineWithPollingSupport(SkipMixin, metaclass=PipelineMeta):
    """
    A pipeline that combines the 'fetch' and 'insert' steps of a standard pipeline, and uses pandas DataFrames to
    efficiently insert large amounts of data to the target table.

    This pipeline should currently only be used when the provided optimisation is required.

    There is also support for a 'polling' step at the start of the pipeline, which will continuously check
    (within a given period) for updates to a dataset and then aims to process and upload that data as fast as
    practicable, to support more time-sensitive workflows. This polling step could probably be moved to the
    standard pipeline eventually - but at the moment it is not needed there.
    """

    target_db: str = config.DATASETS_DB_NAME
    start_date: datetime = datetime(2019, 11, 5)
    end_date: Optional[datetime] = None
    catchup: bool = False

    # Enable or disable Slack notification when DAG run finishes
    alert_on_success: bool = False
    alert_on_failure: bool = True

    # Disables the null columns check that makes sure all DB columns
    # have at least one non-null value
    allow_null_columns: bool = False

    # These two functions must be defined on any subclasses
    date_checker: Callable[[], Tuple[datetime, Optional[datetime]]]
    data_getter: Callable

    table_config: SingleTableConfig

    # Whether to use a polling task to check for new data over a long period of time, as opposed to just
    # assuming new data is available and ingesting immediately.
    use_polling = True

    # How often to poll the data source to read it's "last modified" date.
    polling_interval_in_seconds = 60

    schedule_interval = "0 6 * * *"
    daily_end_time_utc = time(17, 0, 0)

    # Which worker to run the poll/scrape/clean/load task on.
    worker_queue = 'default'

    # If this is defined, it should point to an environment variable that provides a small blob of JSON data:
    # {
    #   "dataset_name": "A friendly name for the dataset",
    #   "dataset_url": "https://www.data.trade.gov.uk/datasets/...",
    #   "emails": ["subscriber@data.trade.gov.uk", ...]
    # }
    update_emails_data_environment_variable: Optional[str] = None

    @classmethod
    def fq_table_name(cls):
        return f'"{cls.table_config.schema}"."{cls.table_config.table_name}"'

    def skip_downstream_tasks(self, **kwargs):
        downstream_tasks = kwargs['task'].get_flat_relatives(upstream=False)
        self.skip(kwargs['dag_run'], kwargs['ti'].execution_date, downstream_tasks)

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 3,
                "retry_delay": timedelta(seconds=30),
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

        if self.use_polling:
            _poll_for_updates = PythonOperator(
                task_id='poll-for-new-data',
                python_callable=poll_for_new_data,
                op_kwargs=dict(
                    target_db=self.target_db,
                    table_config=self.table_config,
                    pipeline_instance=self,
                ),
                dag=dag,
                provide_context=True,
            )
        else:
            _poll_for_updates = None

        _scrape_load_and_check = PythonOperator(
            task_id='scrape-and-load-data',
            python_callable=scrape_load_and_check_data,
            op_kwargs=dict(
                target_db=self.target_db,
                table_config=self.table_config,
                pipeline_instance=self,
            ),
            dag=dag,
            provide_context=True,
            queue=self.worker_queue,
        )

        _swap_dataset_tables = PythonOperator(
            task_id="swap-dataset-table",
            dag=dag,
            python_callable=swap_dataset_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
        )

        _send_dataset_updated_emails = None
        if self.update_emails_data_environment_variable:
            _send_dataset_updated_emails = PythonOperator(
                task_id='send-dataset-updated-emails',
                dag=dag,
                retries=0,
                python_callable=send_dataset_update_emails,
                op_kwargs=dict(
                    update_emails_data_environment_variable=self.update_emails_data_environment_variable
                ),
            )

        _drop_temp_tables = PythonOperator(
            task_id="drop-temp-tables",
            dag=dag,
            python_callable=drop_temp_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            trigger_rule="one_failed",
            op_args=[self.target_db, *self.table_config.tables],
        )

        _drop_swap_tables = PythonOperator(
            task_id="drop-swap-tables",
            dag=dag,
            python_callable=drop_swap_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
        )

        if _poll_for_updates:
            _poll_for_updates >> _scrape_load_and_check

        (
            _scrape_load_and_check
            >> _swap_dataset_tables
            >> [_drop_swap_tables, _drop_temp_tables]
        )

        if _send_dataset_updated_emails:
            _swap_dataset_tables >> _send_dataset_updated_emails

        return dag
