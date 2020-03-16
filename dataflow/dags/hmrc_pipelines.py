"""A module that defines Airflow DAGS for HMRC pipelines"""
import sqlalchemy as sa

from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.operators.hmrc import fetch_uktradeinfo_non_eu_data
from dataflow.utils import TableConfig


class _HMRCPipeline(_PipelineDAG):
    base_filename: str

    table_config: TableConfig

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-uktradeinfo-non-eu-exports",
            python_callable=fetch_uktradeinfo_non_eu_data,
            provide_context=True,
            op_args=[self.table_config.table_name, self.base_filename],
        )


class HMRCNonEUExports(_HMRCPipeline):
    base_filename = "SMKE19"
    table_config = TableConfig(
        table_name="non_eu_exports_hmrc",
        # https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKE19.DOC
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            (0, sa.Column("comcode", sa.String(9))),
            (1, sa.Column("sitc", sa.String(5))),
            (2, sa.Column("record_type", sa.String(3))),
            (3, sa.Column("cod_sequence", sa.String(3))),
            (4, sa.Column("cod_alpha", sa.String(2))),
            (5, sa.Column("account_mmyy", sa.String(7))),
            (6, sa.Column("port_sequence", sa.String(3))),
            (7, sa.Column("port_alpha", sa.String(3))),
            (8, sa.Column("flag_sequence", sa.String(3))),
            (9, sa.Column("flag_alpha", sa.String(2))),
            (10, sa.Column("trade_indicator", sa.String(1))),
            (11, sa.Column("container", sa.String(3))),
            (12, sa.Column("mode_of_transport", sa.String(3))),
            (13, sa.Column("inland_mot", sa.String(2))),
            (14, sa.Column("golo_sequence", sa.String(3))),
            (15, sa.Column("golo_alpha", sa.String(3))),
            (16, sa.Column("suite_indicator", sa.String(3))),
            (17, sa.Column("procedure_code", sa.String(3))),
            (18, sa.Column("value", sa.BigInteger)),
            (19, sa.Column("quantity_1", sa.BigInteger)),
            (20, sa.Column("quantity_2", sa.BigInteger)),
            (21, sa.Column("industrial_plant_comcode", sa.String(15))),
        ],
    )


class HMRCNonEUImports(_HMRCPipeline):
    base_filename = "SMKI19"
    table_config = TableConfig(
        table_name="non_eu_imports_hmrc",
        # https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKI19.DOC
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            (0, sa.Column("comcode", sa.String(9))),
            (1, sa.Column("sitc", sa.String(5))),
            (2, sa.Column("record_type", sa.String(3))),
            (3, sa.Column("cod_sequence", sa.String(3))),
            (4, sa.Column("cod_alpha", sa.String(2))),
            (5, sa.Column("coo_sequence", sa.String(3))),
            (6, sa.Column("coo_alpha", sa.String(2))),
            (7, sa.Column("account_mmyy", sa.String(7))),
            (8, sa.Column("port_sequence", sa.String(3))),
            (9, sa.Column("port_alpha", sa.String(3))),
            (10, sa.Column("flag_sequence", sa.String(3))),
            (11, sa.Column("flag_alpha", sa.String(2))),
            (12, sa.Column("country_sequence", sa.String(3))),
            (13, sa.Column("country_alpha", sa.String(2))),
            (14, sa.Column("trade_indicator", sa.String(1))),
            (15, sa.Column("container", sa.String(3))),
            (16, sa.Column("mode_of_transport", sa.String(3))),
            (17, sa.Column("inland_mot", sa.String(2))),
            (18, sa.Column("golo_sequence", sa.String(3))),
            (19, sa.Column("golo_alpha", sa.String(3))),
            (20, sa.Column("suite_indicator", sa.String(3))),
            (21, sa.Column("procedure_code", sa.String(3))),
            (22, sa.Column("cb_code", sa.String(3))),
            (23, sa.Column("value", sa.BigInteger)),
            (24, sa.Column("quantity_1", sa.BigInteger)),
            (25, sa.Column("quantity_2", sa.BigInteger)),
        ],
    )
