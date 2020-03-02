"""A module that defines Airflow DAGS for HMRC pipelines"""
import sqlalchemy as sa

from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.operators.hmrc import fetch_from_uktradeinfo


class _HMRCPipeline(_PipelineDAG):

    filename: str

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-uktradeinfo-non-eu-exports",
            python_callable=fetch_from_uktradeinfo,
            provide_context=True,
            op_args=[self.table_name, self.filename],
        )


class HMRCNonEUExports(_HMRCPipeline):
    name = "non-eu-exports"
    table_name = "non_eu_exports_hmrc"
    filename = f""
    field_mapping = [
        ("id", sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
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
