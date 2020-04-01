from datetime import datetime
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow import config
from dataflow.config import MARKET_ACCESS_HAWK_CREDENTIALS
from dataflow.dags import _PipelineDAG

from dataflow.operators.common import fetch_from_hawk_api
from dataflow.utils import TableConfig


class MarketAccessTradeBarriersPipeline(_PipelineDAG):
    target_db = config.DATASETS_DB_NAME
    source_url = f"{config.MARKET_ACCESS_BASE_URL}/barriers/dataset"
    start_date = datetime(2020, 3, 18)
    schedule_interval = '@daily'

    table_config = TableConfig(
        table_name="market_access_trade_barriers",
        field_mapping=[
            ("id", sa.Column("id", UUID, primary_key=True)),
            ("code", sa.Column("code", sa.Text)),
            ("scope", sa.Column("scope", sa.Text)),
            ("status", sa.Column("status", sa.Text)),
            ("barrier_title", sa.Column("barrier_title", sa.Text)),
            ("sectors", sa.Column("sectors", sa.ARRAY(sa.Text))),
            ('overseas_region', sa.Column('overseas_region', sa.Text)),
            ('country', sa.Column('country', sa.ARRAY(sa.Text))),
            ('admin_areas', sa.Column('admin_areas', sa.ARRAY(sa.Text))),
            ('categories', sa.Column('categories', sa.ARRAY(sa.Text))),
            ('product', sa.Column('product', sa.Text)),
            ('source', sa.Column('source', sa.Text)),
            ('priority', sa.Column('priority', sa.Text)),
            ('reported_on', sa.Column('reported_on', sa.DateTime)),
            ('modified_on', sa.Column('modified_on', sa.DateTime)),
            ('assessment_impact', sa.Column('assessment_impact', sa.Text)),
            ('value_to_economy', sa.Column('value_to_economy', sa.BigInteger)),
            ('import_market_size', sa.Column('import_market_size', sa.BigInteger)),
            ('commercial_value', sa.Column('commercial_value', sa.BigInteger)),
            ('export_value', sa.Column('export_value', sa.BigInteger)),
            ('resolved_date', sa.Column('resolved_date', sa.Date)),
            ('team_count', sa.Column('team_count', sa.Integer)),
            ('company_names', sa.Column('company_names', sa.ARRAY(sa.Text))),
            ('company_ids', sa.Column('company_ids', sa.ARRAY(sa.Text))),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_hawk_api, hawk_credentials=MARKET_ACCESS_HAWK_CREDENTIALS,
            ),
            provide_context=True,
            op_args=[self.table_config.table.name, self.source_url],
        )
