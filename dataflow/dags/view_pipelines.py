"""A module that defines Airflow DAGS for view pipelines."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import constants
from dataflow.dags.dataset_pipelines import OMISDatasetPipeline
from dataflow.operators.db_view import create_view, list_all_views
from dataflow.utils import XCOMIntegratedPostgresOperator


class BaseViewPipeline:
    start_date = datetime(2019, 10, 1)
    end_date = None
    catchup = True

    schedule_interval = '0 5 1 * *'

    @classmethod
    def get_dag(pipeline):
        user_defined_macros = {
            'view_name': pipeline.view_name,
            'table_name': pipeline.dataset_pipeline.table_name,
            'join_clause': getattr(pipeline, 'join_clause', ''),
            'fields': pipeline.fields,
        }
        if getattr(pipeline, 'params', None):
            user_defined_macros.update(pipeline.params)

        if pipeline.fields == '__all__':
            user_defined_macros['fields'] = [
                (field_name, field_name)
                for _, field_name, _ in pipeline.dataset_pipeline.field_mapping
            ]

        dag = DAG(
            pipeline.__name__,
            catchup=pipeline.catchup,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            },
            start_date=pipeline.start_date,
            end_date=pipeline.end_date,
            schedule_interval=pipeline.schedule_interval,
            user_defined_macros=user_defined_macros,
        )

        dag << PostgresOperator(
            task_id='create-view',
            sql=create_view + pipeline.where_clause,
            postgres_conn_id=pipeline.dataset_pipeline.target_db,
        )
        if constants.DEBUG:
            dag << XCOMIntegratedPostgresOperator(
                task_id='list-views',
                sql=list_all_views,
                postgres_conn_id=pipeline.dataset_pipeline.target_db,
            )

        return dag


class CompletedOMISOrderViewPipeline(BaseViewPipeline):
    """Pipeline meta object for Completed OMIS Order View."""

    view_name = 'completed_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    fields = [
        ('omis_dataset.omis_order_reference', 'OMIS Order Reference'),
        ('companies_dataset.name', 'Company name'),
        ('teams_dataset.name', 'DIT Team'),
        ('ROUND(omis_dataset.subtotal::numeric/100::numeric,2)', 'Net price'),
        ('omis_dataset.uk_region', 'UK Region'),
        ('omis_dataset.market', 'Market'),
        ('omis_dataset.sector', 'Sector'),
        ('omis_dataset.services', 'Services'),
        (
            'to_char(omis_dataset.delivery_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Delivery date',
        ),
        (
            'to_char(omis_dataset.payment_received_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Payment received date',
        ),
        (
            'to_char(omis_dataset.completion_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Completion Date',
        ),
        (
            'to_char(omis_dataset.created_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Created date',
        ),
        (
            'to_char(omis_dataset.cancelled_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Cancelled date',
        ),
        ('omis_dataset.cancellation_reason', 'Cancellation reason'),
    ]
    join_clause = '''
        LEFT JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        LEFT JOIN teams_dataset ON omis_dataset.dit_team_id=teams_dataset.id
    '''
    where_clause = """
        omis_dataset.order_status = 'complete'
        AND date_trunc('month', omis_dataset.completion_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ORDER BY omis_dataset.completion_date
    """


class CancelledOMISOrderViewPipeline(BaseViewPipeline):
    """Pipeline meta object for Cancelled OMIS Order View."""

    view_name = 'cancelled_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    fields = [
        ('omis_dataset.omis_order_reference', 'OMIS Order Reference'),
        ('companies_dataset.name', 'Company Name'),
        ('ROUND(omis_dataset.subtotal::numeric/100::numeric,2)', 'Net price'),
        ('teams_dataset.name', 'DIT Team'),
        ('omis_dataset.market', 'Market'),
        (
            'to_char(omis_dataset.created_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Created Date',
        ),
        (
            'to_char(omis_dataset.cancelled_date, \'YYYY-MM-DD HH24:MI:SS\')',
            'Cancelled Date',
        ),
        ('omis_dataset.cancellation_reason', 'Cancellation reason'),
    ]
    join_clause = """
        LEFT JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        LEFT JOIN teams_dataset ON omis_dataset.dit_team_id=teams_dataset.id
    """
    where_clause = """
        omis_dataset.order_status = 'cancelled'
        AND omis_dataset.cancelled_date >=
        {% if macros.datetime.strptime(ds, '%Y-%m-%d') <= macros.datetime.strptime('{0}-{1}'.format(macros.ds_format(ds, '%Y-%m-%d', '%Y'), month_day_financial_year), '%Y-%m-%d') %}
            to_date('{{ macros.ds_format(macros.ds_add(ds, -365), '%Y-%m-%d', '%Y') }}-{{ month_day_financial_year }}', 'YYYY-MM-DD')
        {% else %}
            to_date('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}-{{ month_day_financial_year }}', 'YYYY-MM-DD')
        {% endif %}
        ORDER BY omis_dataset.cancelled_date
    """
    params = {'month_day_financial_year': constants.FINANCIAL_YEAR_FIRST_MONTH_DAY}


class OMISClientSurveyViewPipeline(BaseViewPipeline):
    """Pipeline meta object for OMIS Client Survey View."""

    view_name = 'omis_client_survey'
    dataset_pipeline = OMISDatasetPipeline
    fields = [
        ('companies_dataset.name', 'Company Name'),
        ('contacts_dataset.contact_name', 'Contact Name'),
        ('contacts_dataset.phone', 'Contact Phone Number'),
        ('contacts_dataset.email', 'Contact Email'),
        ('companies_dataset.address_1', 'Company Trading Address Line 1'),
        ('companies_dataset.address_2', 'Company Trading Address Line 2'),
        ('companies_dataset.address_town', 'Company Trading Address Town'),
        ('companies_dataset.address_county', 'Company Trading Address County'),
        ('companies_dataset.address_country', 'Company Trading Address Country'),
        ('companies_dataset.address_postcode', 'Company Trading Address Postcode'),
        ('companies_dataset.registered_address_1', 'Company Registered Address Line 1'),
        ('companies_dataset.registered_address_2', 'Company Registered Address Line 2'),
        (
            'companies_dataset.registered_address_town',
            'Company Registered Address Town',
        ),
        (
            'companies_dataset.registered_address_county',
            'Company Registered Address County',
        ),
        (
            'companies_dataset.registered_address_country',
            'Company Registered Address Country',
        ),
        (
            'companies_dataset.registered_address_postcode',
            'Company Registered Address Postcode',
        ),
    ]
    join_clause = """
        JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        JOIN contacts_dataset ON omis_dataset.contact_id=contacts_dataset.id
    """
    where_clause = """
        omis_dataset.order_status = 'complete'
        AND date_trunc('month', omis_dataset.completion_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ORDER BY omis_dataset.completion_date
    """


for pipeline in BaseViewPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline.get_dag()
