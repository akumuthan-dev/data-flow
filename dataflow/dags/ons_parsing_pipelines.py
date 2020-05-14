import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

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
            ("Period", sa.Column("period", sa.String)),
            ("Period Type", sa.Column("period_type", sa.String)),
            ("Flow", sa.Column("direction", sa.String)),
            ("Trade Services Code", sa.Column("product_code", sa.String)),
            ("Trade Services Name", sa.Column("product_name", sa.String)),
            ("Value", sa.Column("total", sa.Numeric)),
            ("Unit", sa.Column("unit", sa.String)),
            ("Marker", sa.Column("marker", sa.String)),
        ],
    )
