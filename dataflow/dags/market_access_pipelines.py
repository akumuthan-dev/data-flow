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
    source_url = f"{config.MARKET_ACCESS_BASE_URL}/dataset/v1/barriers"
    start_date = datetime(2020, 3, 18)
    schedule_interval = '@daily'

    table_config = TableConfig(
        table_name="market_access_trade_barriers",
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'sectors': [sector['name'] for sector in record['sectors']],
                'admin_areas': [area['name'] for area in record['admin_areas']],
                'categories': [cat['title'] for cat in record['categories']],
                'company_names': [company['name'] for company in record['companies']]
                if record['companies']
                else [],
                'company_ids': [company['id'] for company in record['companies']]
                if record['companies']
                else [],
            }
        ],
        field_mapping=[
            ("id", sa.Column("id", UUID, primary_key=True)),
            ("code", sa.Column("code", sa.Text)),
            (("term", "name"), sa.Column("term", sa.Text)),
            (("status", "name"), sa.Column("status", sa.Text)),
            (("status", "id"), sa.Column("status_id", sa.Text)),
            ("title", sa.Column("barrier_title", sa.Text)),
            ("sectors", sa.Column("sectors", sa.ARRAY(sa.Text))),
            (
                ('country', 'overseas_region', 'name'),
                sa.Column('overseas_region', sa.Text),
            ),
            (('country', 'name'), sa.Column('country', sa.Text)),
            ('admin_areas', sa.Column('admin_areas', sa.ARRAY(sa.Text))),
            ('categories', sa.Column('categories', sa.ARRAY(sa.Text))),
            ('product', sa.Column('product', sa.Text)),
            (('source', 'name'), sa.Column('source', sa.Text)),
            (('priority', 'name'), sa.Column('priority', sa.Text)),
            ('created_on', sa.Column('reported_on', sa.DateTime)),
            ('modified_on', sa.Column('modified_on', sa.DateTime)),
            (('assessment', 'impact', 'name'), sa.Column('assessment_impact', sa.Text)),
            (
                ('assessment', 'value_to_economy'),
                sa.Column('value_to_economy', sa.BigInteger),
            ),
            (
                ('assessment', 'import_market_size'),
                sa.Column('import_market_size', sa.BigInteger),
            ),
            (
                ('assessment', 'commercial_value'),
                sa.Column('commercial_value', sa.BigInteger),
            ),
            (('assessment', 'export_value'), sa.Column('export_value', sa.BigInteger)),
            ('team_count', sa.Column('team_count', sa.Integer)),
            ('company_names', sa.Column('company_names', sa.ARRAY(sa.Text))),
            ('company_ids', sa.Column('company_ids', sa.ARRAY(sa.Text))),
            (
                ('assessment', 'commercial_value_explanation'),
                sa.Column('commercial_value_explanation', sa.Text),
            ),
            ("archived", sa.Column("archived", sa.Boolean)),
            (("archived_by", "name"), sa.Column("archived_by", sa.Text)),
            ("archived_on", sa.Column("archived_on", sa.DateTime)),
            (("archived_reason", "name"), sa.Column("archived_reason", sa.Text)),
            ("archived_explanation", sa.Column("archived_explanation", sa.Text)),
            # `unarchived_` fields are currently NULL everywhere, so we can't pull them in yet ...
            # (("unarchived_by", "name"), sa.Column("unarchived_by", sa.Text)),
            # ("unarchived_on", sa.Column("unarchived_on", sa.DateTime)),
            # ("unarchived_reason", sa.Column("unarchived_reason", sa.Text)),
            (
                "status_history",
                TableConfig(
                    table_name="market_access_trade_barrier_status_history",
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'trade_barrier_id': contexts[0]['id'],
                        }
                    ],
                    field_mapping=(
                        ('date', sa.Column("date", sa.Text)),
                        (('status', 'name'), sa.Column("status", sa.Text)),
                        (('status', 'id'), sa.Column("status_id", sa.Text)),
                        ('trade_barrier_id', sa.Column('trade_barrier_id', sa.Text)),
                    ),
                ),
            ),
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
