"""A module that defines Airflow DAGS for HMRC pipelines"""
from datetime import timedelta
from typing import Type

import sqlalchemy as sa

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.operators.db_tables import check_table_data
from dataflow.operators.db_tables import create_temp_tables
from dataflow.operators.db_tables import drop_temp_tables
from dataflow.operators.db_tables import swap_dataset_table
from dataflow.operators.hmrc import ExportRecord
from dataflow.operators.hmrc import fetch_non_eu_data_from_uktradeinfo
from dataflow.operators.hmrc import ImportRecord
from dataflow.operators.hmrc import insert_data_into_db
from dataflow.operators.hmrc import Record


class _HMRCPipeline(_PipelineDAG):
    base_filename: str
    record_type: Type[Record]

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-uktradeinfo-non-eu-exports",
            python_callable=fetch_non_eu_data_from_uktradeinfo,
            provide_context=True,
            op_args=[self.table_name, self.base_filename],
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
                "catchup": self.catchup,
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
            op_args=[self.target_db, self.table, self.field_mapping, self.record_type],
        )

        _check_tables = PythonOperator(
            task_id="check-temp-table-data",
            python_callable=check_table_data,
            provide_context=True,
            op_args=[self.target_db, self.table],
            op_kwargs={"allow_null_columns": self.allow_null_columns},
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
        )

        (
            [_fetch, _create_tables]
            >> _insert_into_temp_table
            >> _check_tables
            >> _swap_dataset_table
            >> _drop_tables
        )

        return dag


class HMRCNonEUExports(_HMRCPipeline):
    table_name = "non_eu_exports_hmrc"
    base_filename = "SMKE19"
    record_type = ExportRecord
    field_mapping = [
        (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
        ("comcode", sa.Column("comcode", sa.String(9))),
        ("sitc", sa.Column("sitc", sa.String(5))),
        ("record_type", sa.Column("record_type", sa.String(3))),
        ("cod_sequence", sa.Column("cod_sequence", sa.String(3))),
        ("cod_alpha", sa.Column("cod_alpha", sa.String(2))),
        ("account_mm", sa.Column("account_mm", sa.Integer)),
        ("account_ccyy", sa.Column("account_ccyy", sa.Integer)),
        ("port_sequence", sa.Column("port_sequence", sa.String(3))),
        ("port_alpha", sa.Column("port_alpha", sa.String(3))),
        ("flag_sequence", sa.Column("flag_sequence", sa.String(3))),
        ("flag_alpha", sa.Column("flag_alpha", sa.String(2))),
        ("trade_indicator", sa.Column("trade_indicator", sa.String(1))),
        ("container", sa.Column("container", sa.String(3))),
        ("mode_of_transport", sa.Column("mode_of_transport", sa.String(3))),
        ("inland_mot", sa.Column("inland_mot", sa.String(2))),
        ("golo_sequence", sa.Column("golo_sequence", sa.String(3))),
        ("golo_alpha", sa.Column("golo_alpha", sa.String(3))),
        ("suite_indicator", sa.Column("suite_indicator", sa.String(3))),
        ("procedure_code", sa.Column("procedure_code", sa.String(3))),
        ("value", sa.Column("value", sa.BigInteger)),
        ("quantity_1", sa.Column("quantity_1", sa.BigInteger)),
        ("quantity_2", sa.Column("quantity_2", sa.BigInteger)),
        (
            "industrial_plant_comcode",
            sa.Column("industrial_plant_comcode", sa.String(15)),
        ),
    ]


class HMRCNonEUImports(_HMRCPipeline):
    table_name = "non_eu_imports_hmrc"
    base_filename = "SMKI19"
    record_type = ImportRecord
    field_mapping = [
        (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
        ("comcode", sa.Column("comcode", sa.String(9))),
        ("sitc", sa.Column("sitc", sa.String(5))),
        ("record_type", sa.Column("record_type", sa.String(3))),
        ("cod_sequence", sa.Column("cod_sequence", sa.String(3))),
        ("cod_alpha", sa.Column("cod_alpha", sa.String(2))),
        ("coo_sequence", sa.Column("coo_sequence", sa.String(3))),
        ("coo_alpha", sa.Column("coo_alpha", sa.String(2))),
        ("account_mm", sa.Column("account_mm", sa.Integer)),
        ("account_ccyy", sa.Column("account_ccyy", sa.Integer)),
        ("port_sequence", sa.Column("port_sequence", sa.String(3))),
        ("port_alpha", sa.Column("port_alpha", sa.String(3))),
        ("flag_sequence", sa.Column("flag_sequence", sa.String(3))),
        ("flag_alpha", sa.Column("flag_alpha", sa.String(2))),
        ("country_sequence", sa.Column("country_sequence", sa.String(3))),
        ("country_alpha", sa.Column("country_alpha", sa.String(2))),
        ("trade_indicator", sa.Column("trade_indicator", sa.String(1))),
        ("container", sa.Column("container", sa.String(3))),
        ("mode_of_transport", sa.Column("mode_of_transport", sa.String(3))),
        ("inland_mot", sa.Column("inland_mot", sa.String(2))),
        ("golo_sequence", sa.Column("golo_sequence", sa.String(3))),
        ("golo_alpha", sa.Column("golo_alpha", sa.String(3))),
        ("suite_indicator", sa.Column("suite_indicator", sa.String(3))),
        ("procedure_code", sa.Column("procedure_code", sa.String(3))),
        ("cb_code", sa.Column("cb_code", sa.String(3))),
        ("value", sa.Column("value", sa.BigInteger)),
        ("quantity_1", sa.Column("quantity_1", sa.BigInteger)),
        ("quantity_2", sa.Column("quantity_2", sa.BigInteger)),
    ]
