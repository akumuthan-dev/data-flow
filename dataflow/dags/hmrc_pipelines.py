"""A module that defines Airflow DAGS for HMRC pipelines"""
from datetime import datetime

import sqlalchemy as sa

from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.operators.hmrc import fetch_hmrc_trade_data
from dataflow.utils import LateIndex, TableConfig


class _HMRCPipeline(_PipelineDAG):
    base_filename: str
    records_start_year: int = 2019
    schedule_interval = '0 5 12 * *'
    start_date = datetime(2020, 3, 11)
    use_utc_now_as_source_modified = True
    num_csv_fields: int

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-hmrc-trade-data",
            python_callable=fetch_hmrc_trade_data,
            provide_context=True,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.base_filename,
                self.records_start_year,
                self.num_csv_fields,
            ],
            retries=self.fetch_retries,
        )


class HMRCNonEUExports(_HMRCPipeline):
    base_filename = "smke19"
    records_start_year = 2009
    num_csv_fields = 22
    table_config = TableConfig(
        schema="hmrc",
        table_name="non_eu_exports",
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
            (22, sa.Column("_source_name", sa.String())),
        ],
    )


class HMRCNonEUImports(_HMRCPipeline):
    base_filename = "smki19"
    records_start_year = 2009
    num_csv_fields = 26
    table_config = TableConfig(
        schema="hmrc",
        table_name="non_eu_imports",
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
            (26, sa.Column("_source_name", sa.String())),
        ],
    )


class HMRCEUExports(_HMRCPipeline):
    base_filename = "smkx46"
    records_start_year = 2009
    num_csv_fields = 17
    table_config = TableConfig(
        schema="hmrc",
        table_name="eu_exports",
        # https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKX46.DOC
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            (0, sa.Column("comcode", sa.String(9))),
            (1, sa.Column("record_type", sa.String(3))),
            (2, sa.Column("cod_sequence", sa.String(3))),
            (3, sa.Column("cod_alpha", sa.String(2))),
            (4, sa.Column("trade_indicator", sa.String(1))),
            (5, sa.Column("coo_seq", sa.String(3))),
            (6, sa.Column("coo_alpha", sa.String(2))),
            (7, sa.Column("nature_of_transaction", sa.String(3))),
            (8, sa.Column("mode_of_transport", sa.String(3))),
            (9, sa.Column("period_reference", sa.String(7))),
            (10, sa.Column("suite_indicator", sa.String(3))),
            (11, sa.Column("sitc", sa.String(5))),
            (12, sa.Column("ip_comcode", sa.String(9))),
            (13, sa.Column("num_consignments", sa.BigInteger)),
            (14, sa.Column("value", sa.BigInteger)),
            (15, sa.Column("nett_mass", sa.BigInteger)),
            (16, sa.Column("supp_unit", sa.BigInteger)),
            (17, sa.Column("_source_name", sa.String())),
        ],
    )


class HMRCEUImports(_HMRCPipeline):
    base_filename = "smkm46"
    records_start_year = 2009
    num_csv_fields = 17
    table_config = TableConfig(
        schema="hmrc",
        table_name="eu_imports",
        # https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKX46.DOC
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            (0, sa.Column("comcode", sa.String(9))),
            (1, sa.Column("record_type", sa.String(3))),
            (2, sa.Column("cod_sequence", sa.String(3))),
            (3, sa.Column("cod_alpha", sa.String(2))),
            (4, sa.Column("trade_indicator", sa.String(1))),
            (5, sa.Column("coo_seq", sa.String(3))),
            (6, sa.Column("coo_alpha", sa.String(2))),
            (7, sa.Column("nature_of_transaction", sa.String(3))),
            (8, sa.Column("mode_of_transport", sa.String(3))),
            (9, sa.Column("period_reference", sa.String(7))),
            (10, sa.Column("suite_indicator", sa.String(3))),
            (11, sa.Column("sitc", sa.String(5))),
            (12, sa.Column("ip_comcode", sa.String(9))),
            (13, sa.Column("num_consignments", sa.BigInteger)),
            (14, sa.Column("value", sa.BigInteger)),
            (15, sa.Column("nett_mass", sa.BigInteger)),
            (16, sa.Column("supp_unit", sa.BigInteger)),
            (17, sa.Column("_source_name", sa.String())),
        ],
    )


class _HMRCEUEstimates(_HMRCPipeline):
    records_start_year = 2009
    # The data is in fixed-width format, so it looks like a single field. The source file name is
    # added as a second field by the operator
    num_csv_fields = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table_config = TableConfig(
            schema="hmrc",
            table_name=self.table_name,
            transforms=[
                # This is based on https://www.uktradeinfo.com/media/vrehes3e/tech_spec_sesa16.doc,
                # which has an "a" in the file name which doesn't match the files we use, but it's the
                # best we have
                lambda record, table_config, contexts: {
                    "sitc_2": record[0][0:2],
                    "sitc_0": record[0][2:3],
                    "year": record[0][3:7],
                    "month": record[0][7:9],
                    "estimated_value": record[0][9:24],
                    "_source_name": record[1],
                }
            ],
            field_mapping=[
                (
                    None,
                    sa.Column(
                        "id", sa.BigInteger, primary_key=True, autoincrement=True
                    ),
                ),
                ("sitc_2", sa.Column("sitc_2", sa.String(2), nullable=False)),
                ("sitc_0", sa.Column("sitc_0", sa.String(1), nullable=False)),
                ("year", sa.Column("year", sa.SmallInteger, nullable=False)),
                ("month", sa.Column("month", sa.SmallInteger, nullable=False)),
                (
                    "estimated_value",
                    sa.Column("estimated_value", sa.BigInteger, nullable=False),
                ),
                ("_source_name", sa.Column("_source_name", sa.String, nullable=False)),
            ],
            indexes=[
                LateIndex("sitc_2"),
                LateIndex("sitc_0"),
                LateIndex(["year", "month"]),
                LateIndex("month"),
                LateIndex("_source_name"),
            ],
            check_constraints=[
                sa.CheckConstraint("1 <= month and month <= 12"),
                sa.CheckConstraint("1500 <= year AND year <= 3000"),
                sa.CheckConstraint("sitc_2 ~ '^[0-9][0-9]$'"),
                sa.CheckConstraint("sitc_0 ~ '^[0-9]$'"),
                sa.CheckConstraint("estimated_value >= 0"),
            ],
        )


class HMRCEUExportsEstimates(_HMRCEUEstimates):
    base_filename = "sesx16"
    table_name = "eu_exports_estimates"


class HMRCEUImportsEstimates(_HMRCEUEstimates):
    base_filename = "sesm16"
    table_name = "eu_imports_estimates"
