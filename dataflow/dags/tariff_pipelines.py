from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_api_endpoint
from dataflow.utils import TableConfig


class GlobalUKTariffPipeline(_PipelineDAG):
    source_url = (
        'https://www.check-future-uk-trade-tariffs.service.gov.uk/api/global-uk-tariff'
    )
    table_config = TableConfig(
        schema='dit',
        table_name='global_uk_tariff',
        field_mapping=[
            ('commodity', sa.Column('commodity', sa.String)),
            ('description', sa.Column('description', sa.Text)),
            ('cet_duty_rate', sa.Column('cet_duty_rate', sa.String)),
            ('ukgt_duty_rate', sa.Column('ukgt_duty_rate', sa.String)),
            ('change', sa.Column('change', sa.String)),
            ('trade_remedy_applies', sa.Column('trade_remedy_applies', sa.Boolean)),
            ('dumping_margin_applies', sa.Column('dumping_margin_applies', sa.Boolean)),
            (
                'cet_applies_until_trade_remedy_transition_reviews_concluded',
                sa.Column(
                    'cet_applies_until_trade_remedy_transition_reviews_concluded',
                    sa.Boolean,
                ),
            ),
            ('suspension_applies', sa.Column('suspension_applies', sa.Boolean)),
            ('atq_applies', sa.Column('atq_applies', sa.Boolean)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_api_endpoint, results_key=None, next_key=None
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
        )
