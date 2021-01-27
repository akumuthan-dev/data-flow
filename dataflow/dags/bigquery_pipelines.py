import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.bigquery import fetch_from_bigquery
from dataflow.utils import TableConfig

from datetime import date,timedelta

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
        '''
        field_mapping=[
            ('clientId', sa.Column('clientId', sa.String)),
            ('fullVisitorId', sa.Column('fullVisitorId', sa.String)),
            ('userId', sa.Column('userId', sa.String)),
            ('visitNumber', sa.Column('visitNumber', sa.Numeric)),
            ('visitId', sa.Column('visitId', sa.Numeric)),
            ('visitStartTime', sa.Column('visitStartTime', sa.Numeric)),
            ('date', sa.Column('date', sa.String)),
            ('bounces', sa.Column('bounces', sa.Numeric)),
            ('hits', sa.Column('hits', sa.Numeric)),
            ('newVisits', sa.Column('newVisits', sa.Numeric)),
            ('pageviews', sa.Column('pageviews', sa.Numeric)),
            ('visits', sa.Column('visits', sa.Numeric)),
            ('browser', sa.Column('browser', sa.String)),
            ('browserVersion', sa.Column('browserVersion', sa.String)),
            ('deviceCategory', sa.Column('deviceCategory', sa.String)),
            ('mobileDeviceInfo', sa.Column('mobileDeviceInfo', sa.String)),
            ('mobileDeviceModel', sa.Column('mobileDeviceModel', sa.String)),
            ('operatingSystem', sa.Column('operatingSystem', sa.String)),
            ('operatingSystemVersion', sa.Column('operatingSystemVersion'))
        ],
        '''
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_from_bigquery,
            provide_context=True,
            op_args=[self.table_config.table_name],  # pylint: disable=no-member
            retries=self.fetch_retries,
        )

