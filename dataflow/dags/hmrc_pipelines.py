"""A module that defines Airflow DAGS for HMRC pipelines"""
from datetime import datetime
from typing import Tuple

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
    num_csv_fields: Tuple[int, int]

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


def split_month_year(source_value, month_field, year_field):
    return (
        {year_field: None, month_field: None}
        if source_value == '00/00'
        else {
            year_field: int(source_value.split('/')[1])
            + 2000,  # Assuming only have dates > 2000
            month_field: int(source_value.split('/')[0]),
        }
    )


class HMRCControl(_HMRCPipeline):
    base_filename = "smka12"
    records_start_year = 2009
    # In earlier records, the final column, which has a text description, is split into two
    num_csv_fields = (27, 29)
    table_config = TableConfig(
        schema="hmrc",
        table_name="trade__control",
        transforms=[
            # This is based on https://www.uktradeinfo.com/media/xolb3hjg/tech_spec_smka12.doc
            lambda record, table_config, contexts: {
                'comcode': record[0],
                'eu_noneu_indicator': record[1],
                **split_month_year(record[2], 'eu_on_month', 'eu_on_year'),
                **split_month_year(record[3], 'eu_off_month', 'eu_off_year'),
                **split_month_year(record[4], 'noneu_on_month', 'noneu_on_year'),
                **split_month_year(record[5], 'noneu_off_month', 'noneu_off_year'),
                'non_trade_id': record[6],
                'sitc_no': record[7],
                'sitc_indicator': record[8],
                'sitc_conv_a': record[9],
                'sitc_conv_b': record[10],
                'cn_q2': record[11],
                'supp_arrivals': record[12],
                'supp_dispatches': record[13],
                'supp_imports': record[14],
                'supp_exports': record[15],
                'sub_group_arrivals': record[16],
                'item_arrivals': record[17],
                'sub_group_dispatches': record[18],
                'item_dispatches': record[19],
                'sub_group_imports': record[20],
                'item_imports': record[21],
                'sub_group_exports': record[22],
                'item_exports': record[23],
                'qty1_alpha': record[24],
                'qty2_alpha': record[25],
                'commodity_alpha': record[26]
                + ((' ' + record[27]) if len(record) == 29 else ''),
                '_source_name': record[28] if len(record) == 29 else record[27],
            }
        ],
        # https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads/Tech_Spec_SMKE19.DOC
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ('comcode', sa.Column('comcode', sa.String(9), nullable=False)),
            (
                'eu_noneu_indicator',
                sa.Column('eu_noneu_indicator', sa.String(1), nullable=False),
            ),
            ('eu_on_year', sa.Column('eu_on_year', sa.SmallInteger, nullable=True)),
            ('eu_on_month', sa.Column('eu_on_month', sa.SmallInteger, nullable=True)),
            ('eu_off_year', sa.Column('eu_off_year', sa.SmallInteger, nullable=True)),
            ('eu_off_month', sa.Column('eu_off_month', sa.SmallInteger, nullable=True)),
            (
                'noneu_on_year',
                sa.Column('noneu_on_year', sa.SmallInteger, nullable=True),
            ),
            (
                'noneu_on_month',
                sa.Column('noneu_on_month', sa.SmallInteger, nullable=True),
            ),
            (
                'noneu_off_year',
                sa.Column('noneu_off_year', sa.SmallInteger, nullable=True),
            ),
            (
                'noneu_off_month',
                sa.Column('noneu_off_month', sa.SmallInteger, nullable=True),
            ),
            ('non_trade_id', sa.Column('non_trade_id', sa.String(1), nullable=False)),
            ('sitc_no', sa.Column('sitc_no', sa.String(5), nullable=False)),
            (
                'sitc_indicator',
                sa.Column('sitc_indicator', sa.String(1), nullable=False),
            ),
            ('sitc_conv_a', sa.Column('sitc_conv_a', sa.String(3), nullable=False)),
            ('sitc_conv_b', sa.Column('sitc_conv_b', sa.String(3), nullable=False)),
            ('cn_q2', sa.Column('cn_q2', sa.String(3), nullable=False)),
            ('supp_arrivals', sa.Column('supp_arrivals', sa.String(2), nullable=False)),
            (
                'supp_dispatches',
                sa.Column('supp_dispatches', sa.String(2), nullable=False),
            ),
            ('supp_imports', sa.Column('supp_imports', sa.String(2), nullable=False)),
            ('supp_exports', sa.Column('supp_exports', sa.String(2), nullable=False)),
            (
                'sub_group_arrivals',
                sa.Column('sub_group_arrivals', sa.String(1), nullable=False),
            ),
            ('item_arrivals', sa.Column('item_arrivals', sa.String(1), nullable=False)),
            (
                'sub_group_dispatches',
                sa.Column('sub_group_dispatches', sa.String(1), nullable=False),
            ),
            (
                'item_dispatches',
                sa.Column('item_dispatches', sa.String(1), nullable=False),
            ),
            (
                'sub_group_imports',
                sa.Column('sub_group_imports', sa.String(1), nullable=False),
            ),
            ('item_imports', sa.Column('item_imports', sa.String(1), nullable=False)),
            (
                'sub_group_exports',
                sa.Column('sub_group_exports', sa.String(1), nullable=False),
            ),
            ('item_exports', sa.Column('item_exports', sa.String(1), nullable=False)),
            ('qty1_alpha', sa.Column('qty1_alpha', sa.String(3), nullable=False)),
            ('qty2_alpha', sa.Column('qty2_alpha', sa.String(3), nullable=False)),
            (
                'commodity_alpha',
                sa.Column('commodity_alpha', sa.String(110), nullable=False),
            ),
            ('_source_name', sa.Column('_source_name', sa.String, nullable=False)),
        ],
        indexes=[
            LateIndex("comcode"),
            LateIndex(["eu_on_year", "eu_on_month"]),
            LateIndex("eu_on_month"),
            LateIndex(["eu_off_year", "eu_off_month"]),
            LateIndex("eu_off_month"),
            LateIndex(["noneu_on_year", "noneu_on_month"]),
            LateIndex("noneu_on_month"),
            LateIndex(["noneu_off_year", "noneu_off_month"]),
            LateIndex("noneu_off_month"),
            LateIndex("sitc_no"),
            LateIndex("_source_name"),
        ],
        check_constraints=[
            sa.CheckConstraint("char_length(comcode) = 9"),
            sa.CheckConstraint(
                "eu_off_month IS NULL OR (1 <= eu_off_month AND eu_off_month <= 12)"
            ),
            sa.CheckConstraint(
                "eu_on_month IS NULL OR (1 <= eu_on_month AND eu_on_month <= 12)"
            ),
            sa.CheckConstraint(
                "noneu_off_month IS NULL OR (1 <= noneu_off_month AND noneu_off_month <= 12)"
            ),
            sa.CheckConstraint(
                "noneu_on_month IS NULL OR (1 <= noneu_on_month AND noneu_on_month <= 12)"
            ),
            sa.CheckConstraint(
                "eu_off_year IS NULL OR (2000 <= eu_off_year AND eu_off_year <= 2099)"
            ),
            sa.CheckConstraint(
                "eu_on_year IS NULL OR (2000 <= eu_on_year AND eu_on_year <= 2099)"
            ),
            sa.CheckConstraint(
                "noneu_off_year IS NULL OR (2000 <= noneu_off_year AND noneu_off_year <= 2099)"
            ),
            sa.CheckConstraint(
                "noneu_on_year IS NULL OR (2000 <= noneu_on_year AND noneu_on_year <= 2099)"
            ),
            sa.CheckConstraint("non_trade_id ~ '^[0-9]$'"),
            sa.CheckConstraint("sitc_no ~ '^[0-9]{5}$'"),
            sa.CheckConstraint("sitc_indicator ~ '^[0-9]$'"),
            sa.CheckConstraint("sitc_conv_a ~ '^[0-9]{3}$'"),
            sa.CheckConstraint("sitc_conv_b ~ '^[0-9]{3}$'"),
            sa.CheckConstraint("cn_q2 ~ '^[0-9]{3}$'"),
            sa.CheckConstraint("supp_arrivals ~ '^\\+[0-9]$'"),
            sa.CheckConstraint("supp_dispatches ~ '^\\+[0-9]$'"),
            sa.CheckConstraint("supp_imports ~ '^\\+[0-9]$'"),
            sa.CheckConstraint("supp_exports ~ '^\\+[0-9]$'"),
            sa.CheckConstraint("sub_group_arrivals ~ '^[0-9]$'"),
            sa.CheckConstraint("item_arrivals ~ '^[0-9]$'"),
            sa.CheckConstraint("sub_group_dispatches ~ '^[0-9]$'"),
            sa.CheckConstraint("item_dispatches ~ '^[0-9]$'"),
            sa.CheckConstraint("item_imports ~ '^[0-9]$'"),
            sa.CheckConstraint("item_exports ~ '^[0-9]$'"),
        ],
    )


class HMRCNonEUExports(_HMRCPipeline):
    base_filename = "smke19"
    records_start_year = 2009
    num_csv_fields = (22, 23)
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
    num_csv_fields = (26, 27)
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
    num_csv_fields = (17, 18)
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
    num_csv_fields = (17, 18)
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
    num_csv_fields = (1, 2)

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
