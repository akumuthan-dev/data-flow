import uuid
from datetime import datetime

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow.dags import _PipelineDAG
from dataflow.operators.db_tables import insert_csv_data_into_db
from dataflow.operators.ons import run_ipython_ons_extraction
from dataflow.utils import TableConfig


class _ONSParserPipeline(_PipelineDAG):
    ons_script_dir = "need-to-override"

    def get_insert_data_callable(self):
        return insert_csv_data_into_db

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-ons-parser-script',
            python_callable=run_ipython_ons_extraction,
            provide_context=True,
            op_kwargs=dict(
                table_name=self.table_config.table_name, script_name=self.ons_script_dir
            ),
        )


class ONSUKTradeInServicesByPartnerCountryNSAPipeline(_ONSParserPipeline):
    ons_script_dir = 'uktradeinservicesservicetypebypartnercountrynonseasonallyadjusted'

    table_config = TableConfig(
        table_name="ons_uk_trade_in_services_by_country_nsa",  # dropped "partner" because of table name length limit
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                "norm_period": record["Period"].split("/")[1],
                "norm_period_type": record["Period"].split("/")[0],
                "norm_value": record["Value"]
                or None,  # Convert redacted values ('') to Nones (NULL in DB).
            },
        ],
        field_mapping=[
            (
                None,
                sa.Column(
                    "id", UUID, primary_key=True, default=lambda: str(uuid.uuid4())
                ),
            ),
            (None, sa.Column("import_time", sa.DateTime, default=datetime.utcnow)),
            ("Geography Code", sa.Column("og_ons_iso_alpha_2_code", sa.String)),
            ("Geography Name", sa.Column("og_ons_region_name", sa.String)),
            ("Period", sa.Column("og_period", sa.String)),
            ("norm_period", sa.Column("norm_period", sa.String)),
            ("norm_period_type", sa.Column("norm_period_type", sa.String)),
            ("Flow", sa.Column("og_direction", sa.String)),
            ("Trade Services Code", sa.Column("og_product_code", sa.String)),
            ("Trade Services Name", sa.Column("og_product_name", sa.String)),
            ("Value", sa.Column("og_total", sa.String)),
            ("norm_value", sa.Column("norm_total", sa.Numeric)),
            ("Unit", sa.Column("og_unit", sa.String)),
            ("Marker", sa.Column("og_marker", sa.String)),
        ],
    )


class ONSUKTotalTradeAllCountriesNSA(_ONSParserPipeline):
    start_date = datetime(2020, 4, 1)
    schedule_interval = "@monthly"

    ons_script_dir = 'uktotaltradeallcountriesnonseasonallyadjusted'

    table_config = TableConfig(
        table_name="ons_uk_total_trade_all_countries_nsa",
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                "Period": record["Period"].split("/")[1],
                "Period Type": record["Period"].split("/")[0],
                "Value": record["Value"]
                or None,  # Convert redacted values ('') to Nones (NULL in DB).
            },
        ],
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ("Geography Code", sa.Column("geography_code", sa.String)),
            ("Geography Name", sa.Column("geography_name", sa.String)),
            ("Product", sa.Column("product_name", sa.String)),
            ("Period", sa.Column("period", sa.String)),
            ("Period Type", sa.Column("period_type", sa.String)),
            ("Flow", sa.Column("direction", sa.String)),
            ("Value", sa.Column("total", sa.Numeric)),
            ("Unit", sa.Column("unit", sa.String)),
            ("Marker", sa.Column("marker", sa.String)),
        ],
    )


class ONSUKTradeInGoodsByCountryAndCommodity(_ONSParserPipeline):
    start_date = datetime(2020, 4, 1)
    schedule_interval = "0 0 13 * *"

    ons_script_dir = "uktradecountrybycommodity"

    table_config = TableConfig(
        table_name="ons_uk_trade_in_goods_by_country_commodity",
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                "Period": record["Period"].split("/")[1],
                "Period Type": record["Period"].split("/")[0],
                "Value": record["Value"]
                or None,  # Convert redacted values ('') to Nones (NULL in DB).
                "Marker": "",  # We don't have any markers in this dataset
            },
        ],
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ("Geography Code", sa.Column("geography_code", sa.String)),
            ("Geography Name", sa.Column("geography_name", sa.String)),
            ("Product Code", sa.Column("product_code", sa.String)),
            ("Product Name", sa.Column("product_name", sa.String)),
            ("Period", sa.Column("period", sa.String)),
            ("Period Type", sa.Column("period_type", sa.String)),
            ("Flow", sa.Column("direction", sa.String)),
            ("Value", sa.Column("total", sa.Numeric)),
            ("Unit", sa.Column("unit", sa.String)),
            ("Marker", sa.Column("marker", sa.String)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-ons-parser-script',
            python_callable=run_ipython_ons_extraction,
            provide_context=True,
            queue='high-memory-usage',
            op_kwargs=dict(
                table_name=self.table_config.table_name, script_name=self.ons_script_dir
            ),
        )
