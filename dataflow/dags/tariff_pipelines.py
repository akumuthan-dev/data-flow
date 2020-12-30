from functools import partial
from datetime import datetime, timedelta

import sqlalchemy as sa

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import PipelineMeta, _PipelineDAG
from dataflow.operators.common import fetch_from_api_endpoint
from dataflow.operators.tariff import ingest_uk_tariff_to_temporary_db
from dataflow.utils import TableConfig, slack_alert


class GlobalUKTariffPipeline(_PipelineDAG):
    use_utc_now_as_source_modified = True
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
            retries=self.fetch_retries,
        )


class UkTarrifPipeline(metaclass=PipelineMeta):
    """Ingests the UK Tariff into a temporary database

    The data is supplied as a pgdump, and so the temporary database provides some isolation between
    this an other datasets.

    This is expected to be extended later to a) ingest into the datasets db and b) publish
    to to the public.

    The GlobalUKTariff above (also called the UK Global Tariff) just contains non-preferential,
    rates,  so nothing country-specific. The "UK Tariff" contains the preferential rates from trade
    deals, as well as _export_ measures (as well as other things).
    """

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 3,
                "retry_delay": timedelta(minutes=5),
                "start_date": datetime(2020, 12, 30),
                'catchup': False,
            },
            schedule_interval="@once",
            max_active_runs=1,
            on_failure_callback=partial(slack_alert, success=False),
        )

        PythonOperator(
            provide_context=True,
            dag=dag,
            python_callable=ingest_uk_tariff_to_temporary_db,
            task_id="ingest-uk-tariff-to-temporary-db",
        )

        return dag
