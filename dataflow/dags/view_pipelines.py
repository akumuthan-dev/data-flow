"""A module that defines Airflow DAGS for view pipelines."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import config
from dataflow.dags.dataset_pipelines import (
    OMISDatasetPipeline,
    InteractionsDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
)
from dataflow.operators.db_view import create_view, list_all_views
from dataflow.utils import XCOMIntegratedPostgresOperator


class BaseViewPipeline:
    start_date = datetime(2019, 10, 1)
    end_date = None
    catchup = True

    schedule_interval = '0 5 1 * *'
    materialized = False

    @classmethod
    def get_dag(pipeline):
        user_defined_macros = {
            'view_name': pipeline.view_name,
            'table_name': pipeline.dataset_pipeline.table_name,
            'join_clause': getattr(pipeline, 'join_clause', ''),
            'fields': pipeline.fields,
            'materialized': pipeline.materialized,
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
    materialized = True
    fields = [
        ('omis_dataset.omis_order_reference', 'OMIS Order Reference'),
        ('companies_dataset.name', 'Company name'),
        ('teams_dataset.name', 'DIT Team'),
        ('ROUND(omis_dataset.subtotal::numeric/100::numeric,2)', 'Net price'),
        ('omis_dataset.uk_region', 'UK Region'),
        ('omis_dataset.market', 'Market'),
        ('omis_dataset.sector', 'Sector'),
        ('omis_dataset.services', 'Services'),
        ('to_char(omis_dataset.delivery_date, \'DD/MM/YYYY\')', 'Delivery date'),
        (
            'to_char(omis_dataset.payment_received_date, \'DD/MM/YYYY\')',
            'Payment received date',
        ),
        ('to_char(omis_dataset.completion_date, \'DD/MM/YYYY\')', 'Completion Date'),
        ('to_char(omis_dataset.created_date, \'DD/MM/YYYY\')', 'Created date'),
        ('to_char(omis_dataset.cancelled_date, \'DD/MM/YYYY\')', 'Cancelled date'),
        ('omis_dataset.cancellation_reason', 'Cancellation reason'),
        ('to_char(omis_dataset.refund_created, \'DD/MM/YYYY\')', 'Date of Refund'),
        ('(omis_dataset.refund_total_amount/100)::numeric(15, 2)', 'Refund Amount'),
        ('(omis_dataset.vat_cost/100)::numeric(15, 2)', 'VAT Amount'),
        ('(omis_dataset.total_cost/100)::numeric(15, 2)', 'Gross Amount'),
        ('to_char(omis_dataset.quote_created_on, \'DD/MM/YYYY\')', 'Quote Created'),
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
    materialized = True
    fields = [
        ('omis_dataset.omis_order_reference', 'OMIS Order Reference'),
        ('companies_dataset.name', 'Company Name'),
        ('ROUND(omis_dataset.subtotal::numeric/100::numeric,2)', 'Net price'),
        ('teams_dataset.name', 'DIT Team'),
        ('omis_dataset.market', 'Market'),
        ('to_char(omis_dataset.created_date, \'DD/MM/YYYY\')', 'Created Date'),
        ('to_char(omis_dataset.cancelled_date, \'DD/MM/YYYY\')', 'Cancelled Date'),
        ('omis_dataset.cancellation_reason', 'Cancellation reason'),
        ('to_char(omis_dataset.refund_created, \'DD/MM/YYYY\')', 'Date of Refund'),
        ('(omis_dataset.refund_total_amount/100)::numeric(15, 2)', 'Refund Amount'),
        ('(omis_dataset.vat_cost/100)::numeric(15, 2)', 'VAT Amount'),
        ('(omis_dataset.total_cost/100)::numeric(15, 2)', 'Gross Amount'),
        ('to_char(omis_dataset.quote_created_on, \'DD/MM/YYYY\')', 'Quote Created'),
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
    materialized = True
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
        ('to_char(omis_dataset.refund_created, \'DD/MM/YYYY\')', 'Date of Refund'),
        ('(omis_dataset.refund_total_amount/100)::numeric(15, 2)', 'Refund Amount'),
        ('(omis_dataset.vat_cost/100)::numeric(15, 2)', 'VAT Amount'),
        ('(omis_dataset.total_cost/100)::numeric(15, 2)', 'Gross Amount'),
        ('to_char(omis_dataset.quote_created_on, \'DD/MM/YYYY\')', 'Quote Created'),
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


class OMISAllOrdersViewPipeline(BaseViewPipeline):
    """View pipeline for all OMIS orders created up to the end
     of the last calendar month"""

    view_name = 'all_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    start_date = datetime(2019, 12, 1)
    materialized = True
    fields = [
        ('omis_dataset.omis_order_reference', 'Order ID'),
        ('omis_dataset.order_status', 'Order status'),
        ('companies_dataset.name', 'Company'),
        ('teams_dataset.name', 'Creator team'),
        ('omis_dataset.uk_region', 'UK region'),
        ('omis_dataset.market', 'Primary market'),
        ('omis_dataset.sector', 'Sector'),
        ('companies_dataset.sector', 'Company sector'),
        ('omis_dataset.net_price', 'Net price'),
        ('omis_dataset.services', 'Services'),
        ('omis_dataset.created_date', 'Order created'),
        ('omis_dataset.quote_created_on', 'Quote created'),
        # TODO: enable this once field has been made available on Data Hub
        # ('omis_dataset.quote_accepted_on', 'Quote accepted'),
        ('omis_dataset.delivery_date', 'Planned delivery date'),
        ('omis_dataset.vat_cost', 'VAT'),
        ('omis_dataset.payment_received_date', 'Payment received date'),
        ('omis_dataset.completion_date', 'Completion date'),
        ('omis_dataset.cancelled_date', 'Cancellation date'),
        ('omis_dataset.refund_created', 'Refund date'),
        ('omis_dataset.refund_total_amount', 'Refund amount'),
    ]
    join_clause = '''
        JOIN companies_dataset ON omis_dataset.company_id = companies_dataset.id
        JOIN teams_dataset on omis_dataset.dit_team_id = teams_dataset.id
    '''
    where_clause = '''
        omis_dataset.created_date < date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))        
    '''


class DataHubServiceDeliveryInteractionsViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the data hub service deliveries and interactions report."""

    view_name = 'datahub_service_interactions'
    dataset_pipeline = InteractionsDatasetPipeline
    fields = [
        (
            'to_char(interactions_dataset.interaction_date, \'DD/MM/YYYY\')',
            'Date of Interaction',
        ),
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
        ('to_char(events_dataset.start_date, \'DD/MM/YYYY\')', 'Event Start Date'),
        ('events_dataset.address_town', 'Event Town'),
        ('events_dataset.address_country', 'Event Country'),
        ('events_dataset.uk_region', 'Event UK Region'),
        ('events_dataset.service_name', 'Event Service Name'),
        ('to_char(interactions_dataset.created_on, \'DD/MM/YYYY\')', 'Created On Date'),
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
        (
            'to_char(interactions_dataset.interaction_date, \'DD/MM/YYYY\')',
            'Service Delivery Interaction',
        ),
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
        ('to_char(events_dataset.start_date, \'DD/MM/YYYY\')', 'Event Start Date'),
        ('events_dataset.address_town', 'Event Town'),
        ('events_dataset.address_country', 'Event Country'),
        ('events_dataset.uk_region', 'Event UK Region'),
        ('events_dataset.service_name', 'Event Service Name'),
        ('teams_dataset.role', 'Team Role'),
        ('to_char(interactions_dataset.created_on, \'DD/MM/YYYY\')', 'Created On Date'),
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


class ExportWinsYearlyViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the yearly export wins report."""

    view_name = 'export_wins_yearly'
    dataset_pipeline = ExportWinsWinsDatasetPipeline
    start_date = datetime(2018, 1, 1)
    schedule_interval = '@yearly'
    fields = [
        ('export_wins_wins_dataset.id', 'ID'),
        ('export_wins_wins_dataset.user_name', 'User name'),
        ('export_wins_wins_dataset.user_email', 'User email'),
        ('export_wins_wins_dataset.company_name', 'Company name'),
        (
            'export_wins_wins_dataset.cdms_reference',
            'Data Hub (Companies House) or CDMS reference number',
        ),
        ('export_wins_wins_dataset.customer_name', 'Contact name'),
        ('export_wins_wins_dataset.customer_job_title', 'Job title'),
        ('export_wins_wins_dataset.customer_email_address', 'Contact email'),
        ('export_wins_wins_dataset.customer_location', 'HQ location'),
        (
            'export_wins_wins_dataset.business_type',
            'What kind of business deal was this win?',
        ),
        (
            'export_wins_wins_dataset.description',
            'Summarise the support provided to help achieve this win',
        ),
        ('export_wins_wins_dataset.name_of_customer', 'Overseas customer'),
        ('export_wins_wins_dataset.name_of_export', 'What are the goods or services?'),
        ('to_char(export_wins_wins_dataset.date, \'DD/MM/YYYY\')', 'Date business won'),
        ('export_wins_wins_dataset.country', 'Country'),
        (
            'export_wins_wins_dataset.total_expected_export_value',
            'Total expected export value',
        ),
        (
            'export_wins_wins_dataset.total_expected_non_export_value',
            'Total expected non export value',
        ),
        (
            'export_wins_wins_dataset.total_expected_odi_value',
            'Total expected odi value',
        ),
        (
            'export_wins_wins_dataset.goods_vs_services',
            'Does the expected value relate to',
        ),
        ('export_wins_wins_dataset.sector', 'Sector'),
        (
            'COALESCE(export_wins_wins_dataset.is_prosperity_fund_related, False)',
            'Prosperity Fund',
        ),
        ('export_wins_wins_dataset.hvc', 'HVC code (if applicable)'),
        ('export_wins_wins_dataset.hvo_programme', 'HVO Programme (if applicable)'),
        (
            'COALESCE(export_wins_wins_dataset.has_hvo_specialist_involvement, False)',
            'An HVO specialist was involved',
        ),
        (
            'COALESCE(export_wins_wins_dataset.is_e_exported, False)',
            'E-exporting programme',
        ),
        ('export_wins_wins_dataset.type_of_support_1', 'type of support 1'),
        ('export_wins_wins_dataset.type_of_support_2', 'type of support 2'),
        ('export_wins_wins_dataset.type_of_support_3', 'type of support 3'),
        ('export_wins_wins_dataset.associated_programme_1', 'associated programme 1'),
        ('export_wins_wins_dataset.associated_programme_2', 'associated programme 2'),
        ('export_wins_wins_dataset.associated_programme_3', 'associated programme 3'),
        ('export_wins_wins_dataset.associated_programme_4', 'associated programme 4'),
        ('export_wins_wins_dataset.associated_programme_5', 'associated programme 5'),
        (
            'export_wins_wins_dataset.is_personally_confirmed',
            'I confirm that this information is complete and accurate',
        ),
        (
            'export_wins_wins_dataset.is_line_manager_confirmed',
            'My line manager has confirmed the decision to record this win',
        ),
        ('export_wins_wins_dataset.lead_officer_name', 'Lead officer name'),
        (
            'export_wins_wins_dataset.lead_officer_email_address',
            'Lead officer email address',
        ),
        (
            'export_wins_wins_dataset.other_official_email_address',
            'Secondary email address',
        ),
        ('export_wins_wins_dataset.line_manager_name', 'Line manager'),
        ('export_wins_wins_dataset.team_type', 'team type'),
        ('export_wins_wins_dataset.hq_team', 'HQ team, Region or Post'),
        (
            'export_wins_wins_dataset.business_potential',
            'Medium-sized and high potential companies',
        ),
        ('export_wins_wins_dataset.export_experience', 'Export experience'),
        ('to_char(export_wins_wins_dataset.created, \'DD/MM/YYYY\')', 'Created'),
        ('export_wins_wins_dataset.audit', 'Audit'),
        ('contributing_advisers.advisers', 'Contributing advisors/team'),
        ('export_wins_wins_dataset.customer_email_date', 'Customer email date'),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, \': \', COALESCE(export_breakdowns_1.value, 0))',
            'Export breakdown 1',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, \': \', COALESCE(export_breakdowns_2.value, 0))',
            'Export breakdown 2',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, \': \', COALESCE(export_breakdowns_3.value, 0))',
            'Export breakdown 3',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, \': \', COALESCE(export_breakdowns_4.value, 0))',
            'Export breakdown 4',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, \': \', COALESCE(export_breakdowns_5.value, 0))',
            'Export breakdown 5',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, \': \', COALESCE(non_export_breakdowns_1.value, 0))',
            'Non-export breakdown 1',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, \': \', COALESCE(non_export_breakdowns_2.value, 0))',
            'Non-export breakdown 2',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, \': \', COALESCE(non_export_breakdowns_3.value, 0))',
            'Non-export breakdown 3',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, \': \', COALESCE(non_export_breakdowns_4.value, 0))',
            'Non-export breakdown 4',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, \': \', COALESCE(non_export_breakdowns_5.value, 0))',
            'Non-export breakdown 5',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, \': \', COALESCE(odi_breakdowns_1.value, 0))',
            'Outward Direct Investment breakdown 1',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, \': \', COALESCE(odi_breakdowns_2.value, 0))',
            'Outward Direct Investment breakdown 2',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, \': \', COALESCE(odi_breakdowns_3.value, 0))',
            'Outward Direct Investment breakdown 3',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, \': \', COALESCE(odi_breakdowns_4.value, 0))',
            'Outward Direct Investment breakdown 4',
        ),
        (
            'CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, \': \', COALESCE(odi_breakdowns_5.value, 0))',
            'Outward Direct Investment breakdown 5',
        ),
        ('export_wins_wins_dataset.confirmation_created', 'Date response received'),
        ('export_wins_wins_dataset.confirmation_name', 'Your name'),
        (
            'export_wins_wins_dataset.confirmation_agree_with_win ',
            'Please confirm these details are correct',
        ),
        (
            'export_wins_wins_dataset.confirmation_comments',
            'Other comments or changes to the win details',
        ),
        (
            'export_wins_wins_dataset.confirmation_our_support',
            'Securing the win overall?',
        ),
        (
            'export_wins_wins_dataset.confirmation_access_to_contacts',
            'Gaining access to contacts?',
        ),
        (
            'export_wins_wins_dataset.confirmation_access_to_information',
            'Getting information or improved understanding of the country?',
        ),
        (
            'export_wins_wins_dataset.confirmation_improved_profile',
            'Improving your profile or credibility in the country?',
        ),
        (
            'export_wins_wins_dataset.confirmation_gained_confidence',
            'Having confidence to explore or expand in the country?',
        ),
        (
            'export_wins_wins_dataset.confirmation_developed_relationships',
            'Developing or nurturing critical relationships?',
        ),
        (
            'export_wins_wins_dataset.confirmation_overcame_problem',
            'Overcoming a problem in the country (eg legal, regulatory, commercial)?',
        ),
        (
            'export_wins_wins_dataset.confirmation_involved_state_enterprise',
            'The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)',
        ),
        (
            'export_wins_wins_dataset.confirmation_interventions_were_prerequisite',
            'Our support was a prerequisite to generate this export value',
        ),
        (
            'export_wins_wins_dataset.confirmation_support_improved_speed',
            'Our support helped you achieve this win more quickly',
        ),
        (
            'export_wins_wins_dataset.confirmation_portion_without_help',
            'What value do you estimate you would have achieved without our support?',
        ),
        (
            'export_wins_wins_dataset.confirmation_last_export',
            'Apart from this win, when did your company last export goods or services?',
        ),
        (
            'export_wins_wins_dataset.confirmation_company_was_at_risk_of_not_exporting',
            'If you hadnt achieved this win, your company might have stopped exporting',
        ),
        (
            'export_wins_wins_dataset.confirmation_has_explicit_export_plans',
            'Apart from this win, you already have specific plans to export in the next 12 months',
        ),
        (
            'export_wins_wins_dataset.confirmation_has_enabled_expansion_into_new_market',
            'It enabled you to expand into a new market',
        ),
        (
            'export_wins_wins_dataset.confirmation_has_increased_exports_as_percent_of_turnover',
            'It enabled you to increase exports as a proportion of your turnover',
        ),
        (
            'export_wins_wins_dataset.confirmation_has_enabled_expansion_into_existing_market',
            'It enabled you to maintain or expand in an existing market',
        ),
        (
            'export_wins_wins_dataset.confirmation_case_study_willing',
            'Would you be willing for DIT/Exporting is GREAT to feature your success in marketing materials?',
        ),
        (
            'export_wins_wins_dataset.confirmation_marketing_source',
            'How did you first hear about DIT (or its predecessor, UKTI)',
        ),
        (
            'export_wins_wins_dataset.confirmation_other_marketing_source',
            'Other marketing source',
        ),
    ]
    join_clause = '''
        -- Export breakdowns
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Export'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int
        ) export_breakdowns_1 ON  export_breakdowns_1.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Export'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 1
        ) export_breakdowns_2 ON  export_breakdowns_2.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Export'
            AND export_wins_breakdowns_dataset.year::int = extract(year FROM CURRENT_DATE)::int + 2
        ) export_breakdowns_3 ON  export_breakdowns_3.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Export'
            AND export_wins_breakdowns_dataset.year::int = extract(year FROM CURRENT_DATE)::int + 3
        ) export_breakdowns_4 ON  export_breakdowns_4.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Export'
            AND export_wins_breakdowns_dataset.year::int = extract(year FROM CURRENT_DATE)::int + 4
        ) export_breakdowns_5 ON  export_breakdowns_5.win_id = export_wins_wins_dataset.id
        -- Non export breakdowns
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Non-export'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int
        ) non_export_breakdowns_1 ON  non_export_breakdowns_1.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Non-export'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 1
        ) non_export_breakdowns_2 ON  non_export_breakdowns_2.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Non-export'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 2
        ) non_export_breakdowns_3 ON non_export_breakdowns_3.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Non-export'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 3
        ) non_export_breakdowns_4 ON non_export_breakdowns_4.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Non-export'
            AND export_wins_breakdowns_dataset.year::int = extract(year FROM CURRENT_DATE)::int + 4
        ) non_export_breakdowns_5 ON non_export_breakdowns_5.win_id = export_wins_wins_dataset.id
        -- Outward Direct Investment breakdowns
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int
        ) odi_breakdowns_1 ON  odi_breakdowns_1.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 1
        ) odi_breakdowns_2 ON  odi_breakdowns_2.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 2
        ) odi_breakdowns_3 ON odi_breakdowns_3.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            AND export_wins_breakdowns_dataset.year = extract(year FROM CURRENT_DATE)::int + 3
        ) odi_breakdowns_4 ON odi_breakdowns_4.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT export_wins_breakdowns_dataset.win_id, export_wins_breakdowns_dataset.value
            FROM export_wins_breakdowns_dataset
            WHERE export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            AND export_wins_breakdowns_dataset.year::int = extract(year FROM CURRENT_DATE)::int + 4
        ) odi_breakdowns_5 ON odi_breakdowns_5.win_id = export_wins_wins_dataset.id
        LEFT JOIN (
            SELECT win_id, STRING_AGG(CONCAT('Name: ', name, ', Team: ', team_type, ' - ', hq_team, ' - ', location), ', ') as advisers
            FROM export_wins_advisers_dataset
            GROUP  BY 1
        ) contributing_advisers ON contributing_advisers.win_id = export_wins_wins_dataset.id
    '''
    where_clause = '''
        date_trunc('year', export_wins_wins_dataset.date) =  date_trunc('year', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ORDER BY export_wins_wins_dataset.date
    '''


for pipeline in BaseViewPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline.get_dag()
