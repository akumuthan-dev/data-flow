"""A module that defines Airflow DAGS for view pipelines."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import config
from dataflow.dags.dataset_pipelines import (
    OMISDatasetPipeline,
    InteractionsDatasetPipeline,
)
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
        if config.DEBUG:
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
    params = {'month_day_financial_year': config.FINANCIAL_YEAR_FIRST_MONTH_DAY}


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


class DataHubServiceDeliveryInteractionsViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the data hub service deliveries and interactions report."""

    view_name = 'datahub_service_interactions'
    dataset_pipeline = InteractionsDatasetPipeline
    fields = [
        ('interactions_dataset.interaction_date', 'Date of Interaction'),
        ('interactions_dataset.interaction_kind', 'Interaction Type'),
        ('companies_dataset.name', 'Company Name'),
        ('companies_dataset.company_number', 'Companies HouseID'),
        ('companies_dataset.id', 'Data Hub Company ID'),
        ('companies_dataset.cdms_reference_code', 'CDMS Reference Code'),
        ('companies_dataset.address_postcode', 'Company Postcode'),
        ('companies_dataset.address_1', 'Company Address Line 1'),
        ('companies_dataset.address_2', 'Company Address Line 2'),
        ('companies_dataset.address_town', 'Company Address Town'),
        ('companies_dataset.address_country', 'Company Address Country'),
        ('companies_dataset.website', 'Company Website'),
        ('companies_dataset.number_of_employees', 'Number of Employees'),
        (
            'companies_dataset.is_number_of_employees_estimated',
            'Number of Employees Estimated',
        ),
        ('companies_dataset.turnover', 'Turnover'),
        ('companies_dataset.is_turnover_estimated', 'Turnover Estimated'),
        ('companies_dataset.sector', 'Sector'),
        ('contacts_dataset.contact_name', 'Contact Name'),
        ('contacts_dataset.phone', 'Contact Phone'),
        ('contacts_dataset.email', 'Contact Email'),
        ('contacts_dataset.address_postcode', 'Contact Postcode'),
        ('contacts_dataset.address_1', 'Contact Address Line 1'),
        ('contacts_dataset.address_2', 'Contact Address Line 2'),
        ('contacts_dataset.address_town', 'Contact Address Town'),
        ('contacts_dataset.address_country', 'Contact Address Country'),
        ('advisers_dataset.first_name', 'DIT Adviser First Name'),
        ('advisers_dataset.last_name', 'DIT Adviser Last Name'),
        ('advisers_dataset.telephone_number', 'DIT Adviser Phone'),
        ('advisers_dataset.contact_email', 'DIT Adviser Email'),
        ('teams_dataset.name', 'DIT Team'),
        ('companies_dataset.uk_region', 'Company UK Region'),
        ('interactions_dataset.service_delivery', 'Service Delivery'),
        ('interactions_dataset.interaction_subject', 'Subject'),
        ('interactions_dataset.interaction_notes', 'Notes'),
        ('interactions_dataset.net_company_receipt', 'Net Company Receipt'),
        ('interactions_dataset.grant_amount_offered', 'Grant Amount Offered'),
        ('interactions_dataset.service_delivery_status', 'Service Delivery Status'),
        ('events_dataset.name', 'Event Name'),
        ('events_dataset.event_type', 'Event Type'),
        ('events_dataset.start_date', 'Event Start Date'),
        ('events_dataset.address_town', 'Event Town'),
        ('events_dataset.address_country', 'Event Country'),
        ('events_dataset.uk_region', 'Event UK Region'),
        ('events_dataset.service_name', 'Event Service Name'),
        ('interactions_dataset.created_on', 'Created On Date'),
        ('interactions_dataset.communication_channel', 'Communication Channel'),
        ('interactions_dataset.interaction_link', 'Interaction Link'),
    ]
    join_clause = '''
        JOIN companies_dataset ON interactions_dataset.company_id = companies_dataset.id
        JOIN advisers_dataset ON interactions_dataset.adviser_ids[1]::uuid = advisers_dataset.id
        JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
        LEFT JOIN events_dataset ON interactions_dataset.event_id = events_dataset.id
        JOIN contacts_dataset ON contacts_dataset.id = (
            select contact_id
            FROM (
                SELECT UNNEST(contact_ids)::uuid AS contact_id
                FROM interactions_dataset i
                WHERE interactions_dataset.id=i.id
            ) nested_contacts
            JOIN contacts_dataset ON contacts_dataset.id = contact_id
            ORDER BY contacts_dataset.is_primary DESC NULLS LAST
            LIMIT 1
        )
    '''
    where_clause = '''
        date_trunc('month', interactions_dataset.interaction_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        AND interactions_dataset.interaction_kind = 'service_delivery'
        ORDER BY interactions_dataset.interaction_date
    '''


class DataHubExportClientSurveyViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the data hub export client survey report."""

    view_name = 'datahub_export_client_survey'
    dataset_pipeline = InteractionsDatasetPipeline
    fields = [
        ('interactions_dataset.interaction_date', 'Service Delivery Interaction'),
        ('companies_dataset.name', 'Company Name'),
        ('companies_dataset.company_number', 'Companies House ID'),
        ('companies_dataset.id', 'Data Hub Company ID'),
        ('companies_dataset.cdms_reference_code', 'CDMS Reference Code'),
        ('companies_dataset.address_postcode', 'Company Postcode'),
        ('companies_dataset.company_number', 'Companies HouseID'),
        ('companies_dataset.address_1', 'Company Address Line 1'),
        ('companies_dataset.address_2', 'Company Address Line 2'),
        ('companies_dataset.address_town', 'Company Address Town'),
        ('companies_dataset.address_country', 'Company Address Country'),
        ('companies_dataset.website', 'Company Website'),
        ('companies_dataset.number_of_employees', 'Number of Employees'),
        (
            'companies_dataset.is_number_of_employees_estimated',
            'Number of Employees Estimated',
        ),
        ('companies_dataset.turnover', 'Turnover'),
        ('companies_dataset.is_turnover_estimated', 'Turnover Estimated'),
        ('companies_dataset.sector', 'Sector'),
        ('contacts_dataset.contact_name', 'Contact Name'),
        ('contacts_dataset.phone', 'Contact Phone'),
        ('contacts_dataset.email', 'Contact Email'),
        ('contacts_dataset.address_postcode', 'Contact Postcode'),
        ('contacts_dataset.address_1', 'Contact Address Line 1'),
        ('contacts_dataset.address_2', 'Contact Address Line 2'),
        ('contacts_dataset.address_town', 'Contact Address Town'),
        ('contacts_dataset.address_country', 'Contact Address Country'),
        ('teams_dataset.name', 'DIT Team'),
        ('companies_dataset.uk_region', 'Company UK Region'),
        ('interactions_dataset.service_delivery', 'Service Delivery'),
        ('interactions_dataset.interaction_subject', 'Subject'),
        ('interactions_dataset.interaction_notes', 'Notes'),
        ('interactions_dataset.net_company_receipt', 'Net Company Receipt'),
        ('interactions_dataset.grant_amount_offered', 'Grant Amount Offered'),
        ('interactions_dataset.service_delivery_status', 'Service Delivery Status'),
        ('events_dataset.name', 'Event Name'),
        ('events_dataset.event_type', 'Event Type'),
        ('events_dataset.start_date', 'Event Start Date'),
        ('events_dataset.address_town', 'Event Town'),
        ('events_dataset.address_country', 'Event Country'),
        ('events_dataset.uk_region', 'Event UK Region'),
        ('events_dataset.service_name', 'Event Service Name'),
        ('teams_dataset.role', 'Team Role'),
        ('interactions_dataset.created_on', 'Created On Date'),
    ]
    join_clause = '''
        JOIN companies_dataset ON interactions_dataset.company_id = companies_dataset.id
        JOIN advisers_dataset ON interactions_dataset.adviser_ids[1]::uuid = advisers_dataset.id
        JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
        LEFT JOIN events_dataset ON interactions_dataset.event_id = events_dataset.id
        JOIN contacts_dataset ON contacts_dataset.id = (
            select contact_id
            FROM (
                SELECT UNNEST(contact_ids)::uuid AS contact_id
                FROM interactions_dataset i
                WHERE interactions_dataset.id=i.id
            ) nested_contacts
            JOIN contacts_dataset ON contacts_dataset.id = contact_id
            ORDER BY contacts_dataset.is_primary DESC NULLS LAST
            LIMIT 1
        )
    '''
    where_clause = '''
        interactions_dataset.interaction_kind = 'service_delivery'
        AND date_trunc('month', interactions_dataset.interaction_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ORDER BY interactions_dataset.interaction_date
    '''


for pipeline in BaseViewPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline.get_dag()

