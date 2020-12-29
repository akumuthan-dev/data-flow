"""A module that defines Airflow DAGS for Comtrade pipelines"""

import sqlalchemy as sa

from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG, _PandasPipelineWithPollingSupport
from dataflow.operators.comtrade import (
    fetch_comtrade_goods_data_frames,
    fetch_comtrade_services_data,
)
from dataflow.utils import TableConfig, SingleTableConfig, LateIndex


class ComtradeGoodsPipeline(_PandasPipelineWithPollingSupport):
    use_polling = False

    schedule_interval = "0 1 * * 6"  # Every Saturday morning

    worker_queue = 'high-memory-usage'

    table_config = SingleTableConfig(
        schema="un",
        table_name="comtrade__goods",
        field_mapping=[
            ('Classification', sa.Column('classification', sa.String, nullable=False)),
            ('Year', sa.Column('year', sa.SmallInteger, nullable=False)),
            ('Period', sa.Column('period', sa.SmallInteger, nullable=False)),
            (
                'Period Desc.',
                sa.Column('period_desc', sa.SmallInteger, nullable=False),
            ),
            (
                'Aggregate Level',
                sa.Column('aggregate_level', sa.BigInteger, nullable=False),
            ),
            ('Is Leaf Code', sa.Column('is_leaf_code', sa.Boolean)),
            (
                'Trade Flow Code',
                sa.Column('trade_flow_code', sa.BigInteger, nullable=False),
            ),
            ('Trade Flow', sa.Column('trade_flow', sa.String, nullable=False)),
            (
                'Reporter Code',
                sa.Column('reporter_code', sa.BigInteger, nullable=False),
            ),
            ('Reporter', sa.Column('reporter', sa.String, nullable=False)),
            ('Reporter ISO', sa.Column('reporter_iso', sa.String)),
            ('Partner Code', sa.Column('partner_code', sa.BigInteger, nullable=False)),
            ('Partner', sa.Column('partner', sa.String, nullable=False)),
            ('Partner ISO', sa.Column('partner_iso', sa.String)),
            ('Commodity Code', sa.Column('commodity_code', sa.String, nullable=False)),
            ('Commodity', sa.Column('commodity', sa.String, nullable=False)),
            (
                'Qty Unit Code',
                sa.Column('quantity_unit_code', sa.BigInteger, nullable=False),
            ),
            ('Trade Value (US$)', sa.Column('trade_value_usd', sa.Numeric)),
        ],
        indexes=[
            LateIndex('classification'),
            LateIndex('period'),
            LateIndex('period_desc'),
            LateIndex('year'),
            LateIndex('aggregate_level'),
            LateIndex('is_leaf_code'),
            LateIndex('reporter_code'),
            LateIndex('partner_code'),
        ],
    )

    data_getter = fetch_comtrade_goods_data_frames


class ComtradeServicesPipeline(_PipelineDAG):
    schedule_interval = "0 1 * * 6"  # Every Saturday morning

    allow_null_columns = True

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-comtrade-services-data",
            python_callable=fetch_comtrade_services_data,
            provide_context=True,
            op_args=[self.table_config.table_name],  # pylint: disable=no-member
            retries=self.fetch_retries,
        )

    table_config = TableConfig(
        schema="un",
        table_name="comtrade__services",
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            (
                'Classification',
                sa.Column('classification', sa.String, nullable=False, index=True),
            ),
            ('Year', sa.Column('year', sa.SmallInteger, nullable=False, index=True)),
            (
                'Period',
                sa.Column('period', sa.SmallInteger, nullable=False, index=True),
            ),
            (
                'Period Desc.',
                sa.Column('period_desc', sa.SmallInteger, nullable=False, index=True),
            ),
            (
                'Aggregate Level',
                sa.Column('aggregate_level', sa.BigInteger, nullable=False, index=True),
            ),
            ('is_leaf_code_bool', sa.Column('is_leaf_code', sa.Boolean)),
            (
                'Trade Flow Code',
                sa.Column('trade_flow_code', sa.BigInteger, nullable=False),
            ),
            ('Trade Flow', sa.Column('trade_flow', sa.String, nullable=False)),
            (
                'Reporter Code',
                sa.Column('reporter_code', sa.BigInteger, nullable=False, index=True),
            ),
            ('Reporter', sa.Column('reporter', sa.String, nullable=False)),
            ('Reporter ISO', sa.Column('reporter_iso', sa.String)),
            (
                'Partner Code',
                sa.Column('partner_code', sa.BigInteger, nullable=False, index=True),
            ),
            ('Partner', sa.Column('partner', sa.String, nullable=False)),
            ('Partner ISO', sa.Column('partner_iso', sa.String)),
            ('Commodity Code', sa.Column('commodity_code', sa.String, nullable=False)),
            ('Commodity', sa.Column('commodity', sa.String, nullable=False)),
            ('Trade Value (US$)', sa.Column('trade_value_usd', sa.Numeric)),
        ],
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'is_leaf_code_bool': bool(int(record['Is Leaf Code'])),
            }
        ],
    )
