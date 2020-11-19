"""A module that defines Airflow DAGS for Comtrade pipelines"""

import sqlalchemy as sa

from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.operators.comtrade import (
    fetch_comtrade_goods_data,
    fetch_comtrade_services_data,
)
from dataflow.utils import TableConfig


class ComtradeGoodsPipeline(_PipelineDAG):
    allow_null_columns = True
    table_config = TableConfig(
        schema="un",
        table_name="comtrade__goods",
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ('Classification', sa.Column('classification', sa.String, nullable=False, index=True)),
            ('Year', sa.Column('year', sa.SmallInteger, nullable=False, index=True)),
            ('Aggregate Level', sa.Column('aggregate_level', sa.BigInteger, nullable=False, index=True)),
            ('Is Leaf Code', sa.Column('is_leaf_code', sa.Boolean)),
            ('Trade Flow Code', sa.Column('trade_flow_code', sa.BigInteger, nullable=False)),
            ('Trade Flow', sa.Column('trade_flow', sa.String, nullable=False)),
            ('Reporter Code', sa.Column('reporter_code', sa.BigInteger, nullable=False, index=True)),
            ('Reporter', sa.Column('reporter', sa.String, nullable=False)),
            ('Reporter ISO', sa.Column('reporter_iso', sa.String, nullable=False)),
            ('Partner Code', sa.Column('partner_code', sa.BigInteger, nullable=False, index=True)),
            ('Partner', sa.Column('partner', sa.String, nullable=False)),
            ('Partner ISO', sa.Column('partner_iso', sa.String)),
            ('2nd Partner Code', sa.Column('second_partner_code', sa.BigInteger)),
            ('2nd Partner', sa.Column('second_partner', sa.String)),
            ('2nd Partner ISO', sa.Column('second_partner_iso', sa.String)),
            ('Customs Proc. Code', sa.Column('customs_proc_code', sa.String)),
            ('Customs', sa.Column('customs', sa.String)),
            (
                'Mode of Transport Code',
                sa.Column('mode_of_transport_code', sa.BigInteger),
            ),
            ('Mode of Transport', sa.Column('mode_of_transport', sa.String)),
            ('Commodity Code', sa.Column('commodity_code', sa.String, nullable=False)),
            ('Commodity', sa.Column('commodity', sa.String, nullable=False)),
            ('Qty Unit Code', sa.Column('quantity_unit_code', sa.BigInteger, nullable=False)),
            ('Alt Qty Unit Code', sa.Column('alt_quantity_unit_code', sa.String)),
            ('Gross weight (kg)', sa.Column('gross_weight_kg', sa.Numeric)),
            ('Trade Value (US$)', sa.Column('trade_value_usd', sa.Numeric)),
            ('CIF Trade Value (US$)', sa.Column('cif_trade_value_usd', sa.Numeric)),
            ('FOB Trade Value (US$)', sa.Column('fob_trade_value_usd', sa.Numeric)),
        ],
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'Is Leaf Code': bool(int(record['Is Leaf Code'])),
            }
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-comtrade-goods-data",
            python_callable=fetch_comtrade_goods_data,
            provide_context=True,
            op_args=[self.table_config.table_name],  # pylint: disable=no-member
        )


class ComtradeServicesPipeline(_PipelineDAG):
    table_config = TableConfig(
        schema="un", table_name="comtrade__services", field_mapping=[],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-comtrade-services-data",
            python_callable=fetch_comtrade_services_data,
            provide_context=True,
            op_args=[self.table_config.table_name],  # pylint: disable=no-member
        )
