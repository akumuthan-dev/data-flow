import sys

from datetime import datetime, timedelta
from typing import Optional

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.operators.csv_outputs import create_csv
from dataflow.operators.db_tables import (
    check_table_data,
    create_temp_tables,
    drop_temp_tables,
    insert_data_into_db,
    swap_dataset_table,
)
from dataflow.utils import FieldMapping


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

    table_name: str
    allow_null_columns: bool = False
    cascade_drop_tables: bool = False

    field_mapping: FieldMapping

    def get_fetch_operator(self) -> PythonOperator:
        raise NotImplementedError(
            f"{self.__class__} needs to override get_fetch_operator"
        )

    @property
    def table(self) -> sa.Table:
        if not hasattr(self, "_table"):
            meta = sa.MetaData()
            self._table = sa.Table(
                self.table_name,
                meta,
                *[column.copy() for _, column in self.field_mapping],
            )

        return self._table

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
        )

        _fetch = self.get_fetch_operator()
        _fetch.dag = dag

        _create_tables = PythonOperator(
            task_id="create-temp-tables",
            python_callable=create_temp_tables,
            provide_context=True,
            op_args=[self.target_db, self.table],
            dag=dag,
        )

        _insert_into_temp_table = PythonOperator(
            task_id="insert-into-temp-table",
            python_callable=insert_data_into_db,
            provide_context=True,
            op_args=[self.target_db, self.table, self.field_mapping],
        )

        _check_tables = PythonOperator(
            task_id="check-temp-table-data",
            python_callable=check_table_data,
            provide_context=True,
            op_args=[self.target_db, self.table],
            op_kwargs={'allow_null_columns': self.allow_null_columns},
        )

        _swap_dataset_table = PythonOperator(
            task_id="swap-dataset-table",
            python_callable=swap_dataset_table,
            provide_context=True,
            op_args=[self.target_db, self.table],
        )

        _drop_tables = PythonOperator(
            task_id="drop-temp-tables",
            python_callable=drop_temp_tables,
            provide_context=True,
            trigger_rule="all_done",
            op_args=[self.target_db, self.table],
            op_kwargs={'cascade': self.cascade_drop_tables},
        )

        (
            [_fetch, _create_tables]
            >> _insert_into_temp_table
            >> _check_tables
            >> _swap_dataset_table
            >> _drop_tables
        )

        return dag


class _CSVPipelineDAG(metaclass=PipelineMeta):
    target_db: str = config.DATASETS_DB_NAME
    start_date: datetime = datetime(2019, 11, 5)
    end_date: Optional[datetime] = None
    schedule_interval: str = "@daily"
    catchup: bool = True

    static: bool = False
    timestamp_output: bool = True
    refresh_daily: bool = True

    base_file_name: str
    query: str

    def get_dag(self) -> DAG:
        with DAG(
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
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
        ) as dag:
            PythonOperator(
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

        return dag
