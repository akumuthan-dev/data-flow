import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.bigquery import fetch_from_bigquery
from dataflow.utils import TableConfig


class GreatBetaGoogleAnalyticsPipeline(_PipelineDAG):
    schedule_interval = '0 0 10 * *'
    table_config = TableConfig(
        # These names probably do not adhere to our standards, should be re-named
        schema='google_analytics',
        table_name='great__beta',
        field_mapping=[
            ('id', sa.Column('id', sa.String(8), primary_key=True)),
            ('SomeName', sa.Column('some_name', sa.String)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_from_bigquery,
            provide_context=True,
            op_args=[self.table_config.table_name],  # pylint: disable=no-member
            retries=self.fetch_retries,
        )
