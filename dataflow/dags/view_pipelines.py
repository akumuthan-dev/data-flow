"""A module that defines Airflow DAGS for view pipelines."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import config
from dataflow.dags.dataset_pipelines import (
    OMISDatasetPipeline,
    InteractionsDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
    InvestmentProjectsDatasetPipeline,
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
            'materialized': pipeline.materialized,
        }
        if getattr(pipeline, 'params', None):
            user_defined_macros.update(pipeline.params)

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
            sql=create_view + pipeline.query,
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
    query = '''
        SELECT
            omis_dataset.omis_order_reference AS "OMIS Order Reference",
            companies_dataset.name AS "Company name",
            teams_dataset.name AS "DIT Team",
            ROUND(omis_dataset.subtotal::numeric/100::numeric,2) AS "Net price",
            omis_dataset.uk_region AS "UK Region",
            omis_dataset.market AS "Market",
            omis_dataset.sector AS "Sector",
            omis_dataset.services AS "Services",
            to_char(omis_dataset.delivery_date, 'DD/MM/YYYY') AS "Delivery date",
            to_char(omis_dataset.payment_received_date, 'DD/MM/YYYY') AS "Payment received date",
            to_char(omis_dataset.completion_date, 'DD/MM/YYYY') AS "Completion Date",
            to_char(omis_dataset.created_date, 'DD/MM/YYYY') AS "Created date",
            to_char(omis_dataset.cancelled_date, 'DD/MM/YYYY') AS "Cancelled date",
            omis_dataset.cancellation_reason AS "Cancellation reason",
            to_char(omis_dataset.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis_dataset.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis_dataset.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis_dataset.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis_dataset.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM omis_dataset
        LEFT JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        LEFT JOIN teams_dataset ON omis_dataset.dit_team_id=teams_dataset.id
        WHERE omis_dataset.order_status = 'complete'
        AND date_trunc('month', omis_dataset.completion_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ORDER BY omis_dataset.completion_date
    '''


class CancelledOMISOrderViewPipeline(BaseViewPipeline):
    """Pipeline meta object for Cancelled OMIS Order View."""

    view_name = 'cancelled_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    query = '''
        SELECT
            omis_dataset.omis_order_reference AS "OMIS Order Reference",
            companies_dataset.name AS "Company Name",
            ROUND(omis_dataset.subtotal::numeric/100::numeric,2) AS "Net price",
            teams_dataset.name AS "DIT Team",
            omis_dataset.market AS "Market",
            to_char(omis_dataset.created_date, 'DD/MM/YYYY') AS "Created Date",
            to_char(omis_dataset.cancelled_date, 'DD/MM/YYYY') AS "Cancelled Date",
            omis_dataset.cancellation_reason AS "Cancellation reason",
            to_char(omis_dataset.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis_dataset.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis_dataset.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis_dataset.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis_dataset.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM omis_dataset
        LEFT JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        LEFT JOIN teams_dataset ON omis_dataset.dit_team_id=teams_dataset.id
        WHERE omis_dataset.order_status = 'cancelled'
        AND omis_dataset.cancelled_date >=
        {% if macros.datetime.strptime(ds, '%Y-%m-%d') <= macros.datetime.strptime('{0}-{1}'.format(macros.ds_format(ds, '%Y-%m-%d', '%Y'), month_day_financial_year), '%Y-%m-%d') %}
            to_date('{{ macros.ds_format(macros.ds_add(ds, -365), '%Y-%m-%d', '%Y') }}-{{ month_day_financial_year }}', 'YYYY-MM-DD')
        {% else %}
            to_date('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}-{{ month_day_financial_year }}', 'YYYY-MM-DD')
        {% endif %}
        ORDER BY omis_dataset.cancelled_date
    '''
    params = {'month_day_financial_year': config.FINANCIAL_YEAR_FIRST_MONTH_DAY}


class OMISClientSurveyViewPipeline(BaseViewPipeline):
    """Pipeline meta object for OMIS Client Survey View."""

    view_name = 'omis_client_survey'
    dataset_pipeline = OMISDatasetPipeline
    materialized = True
    query = '''
        SELECT
            companies_dataset.name AS "Company Name",
            contacts_dataset.contact_name AS "Contact Name",
            contacts_dataset.phone AS "Contact Phone Number",
            contacts_dataset.email AS "Contact Email",
            companies_dataset.address_1 AS "Company Trading Address Line 1",
            companies_dataset.address_2 AS "Company Trading Address Line 2",
            companies_dataset.address_town AS "Company Trading Address Town",
            companies_dataset.address_county AS "Company Trading Address County",
            companies_dataset.address_country AS "Company Trading Address Country",
            companies_dataset.address_postcode AS "Company Trading Address Postcode",
            companies_dataset.registered_address_1 AS "Company Registered Address Line 1",
            companies_dataset.registered_address_2 AS "Company Registered Address Line 2",
            companies_dataset.registered_address_town AS "Company Registered Address Town",
            companies_dataset.registered_address_county AS "Company Registered Address County",
            companies_dataset.registered_address_country AS "Company Registered Address Country",
            companies_dataset.registered_address_postcode AS "Company Registered Address Postcode",
            to_char(omis_dataset.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis_dataset.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis_dataset.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis_dataset.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis_dataset.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM omis_dataset
        JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        JOIN contacts_dataset ON omis_dataset.contact_id=contacts_dataset.id
        WHERE omis_dataset.order_status = 'complete'
        AND date_trunc('month', omis_dataset.completion_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ORDER BY omis_dataset.completion_date
    '''


class OMISAllOrdersViewPipeline(BaseViewPipeline):
    """View pipeline for all OMIS orders created up to the end
     of the last calendar month"""

    view_name = 'all_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    start_date = datetime(2019, 12, 1)
    query = '''
        SELECT
            omis_dataset.omis_order_reference AS "Order ID",
            omis_dataset.order_status AS "Order status",
            companies_dataset.name AS "Company",
            teams_dataset.name AS "Creator team",
            omis_dataset.uk_region AS "UK region",
            omis_dataset.market AS "Primary market",
            omis_dataset.sector AS "Sector",
            companies_dataset.sector AS "Company sector",
            omis_dataset.net_price AS "Net price",
            omis_dataset.services AS "Services",
            omis_dataset.created_date AS "Order created",
            omis_dataset.quote_created_on AS "Quote created",
            omis_dataset.quote_accepted_on AS "Quote accepted",
            omis_dataset.delivery_date AS "Planned delivery date",
            omis_dataset.vat_cost AS "VAT",
            omis_dataset.payment_received_date AS "Payment received date",
            omis_dataset.completion_date AS "Completion date",
            omis_dataset.cancelled_date AS "Cancellation date",
            omis_dataset.refund_created AS "Refund date",
            omis_dataset.refund_total_amount AS "Refund amount"
        FROM omis_dataset
        JOIN companies_dataset ON omis_dataset.company_id = companies_dataset.id
        JOIN teams_dataset on omis_dataset.dit_team_id = teams_dataset.id
        WHERE omis_dataset.created_date < date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
    '''


class DataHubServiceDeliveryInteractionsViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the data hub service deliveries and interactions report."""

    view_name = 'datahub_service_interactions'
    dataset_pipeline = InteractionsDatasetPipeline
    start_date = datetime(2019, 11, 15)
    schedule_interval = '0 5 15 * *'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM interactions_dataset
            WHERE interactions_dataset.interaction_kind = 'service_delivery'
            AND date_trunc('month', interactions_dataset.interaction_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM contacts_dataset
            JOIN contact_ids ON contacts_dataset.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, contacts_dataset.is_primary DESC NULLS LAST
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            companies_dataset.name AS "Company Name",
            companies_dataset.company_number AS "Companies HouseID",
            companies_dataset.id AS "Data Hub Company ID",
            companies_dataset.cdms_reference_code AS "CDMS Reference Code",
            companies_dataset.address_postcode AS "Company Postcode",
            companies_dataset.address_1 AS "Company Address Line 1",
            companies_dataset.address_2 AS "Company Address Line 2",
            companies_dataset.address_town AS "Company Address Town",
            companies_dataset.address_country AS "Company Address Country",
            companies_dataset.website AS "Company Website",
            companies_dataset.number_of_employees AS "Number of Employees",
            companies_dataset.is_number_of_employees_estimated AS "Number of Employees Estimated",
            companies_dataset.turnover AS "Turnover",
            companies_dataset.is_turnover_estimated AS "Turnover Estimated",
            companies_dataset.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            advisers_dataset.first_name AS "DIT Adviser First Name",
            advisers_dataset.last_name AS "DIT Adviser Last Name",
            advisers_dataset.telephone_number AS "DIT Adviser Phone",
            advisers_dataset.contact_email AS "DIT Adviser Email",
            teams_dataset.name AS "DIT Team",
            companies_dataset.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            events_dataset.name AS "Event Name",
            events_dataset.event_type AS "Event Type",
            to_char(events_dataset.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            events_dataset.address_town AS "Event Town",
            events_dataset.address_country AS "Event Country",
            events_dataset.uk_region AS "Event UK Region",
            events_dataset.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link"
        FROM interactions
        JOIN companies_dataset ON interactions.company_id = companies_dataset.id
        JOIN advisers_dataset ON interactions.adviser_ids[1]::uuid = advisers_dataset.id
        JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
        LEFT JOIN events_dataset ON interactions.event_id = events_dataset.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        ORDER BY interactions.interaction_date
    '''


class DataHubExportClientSurveyViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the data hub export client survey report."""

    view_name = 'datahub_export_client_survey'
    dataset_pipeline = InteractionsDatasetPipeline
    start_date = datetime(2019, 11, 15)
    schedule_interval = '0 5 15 * *'
    materialized = True
    query = '''
        WITH service_deliveries AS (
            SELECT *
            FROM interactions_dataset
            WHERE interactions_dataset.interaction_kind = 'service_delivery'
            AND date_trunc('month', interactions_dataset.interaction_date) = date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD'))
        ),
        contact_ids AS (
            SELECT id AS service_delivery_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM service_deliveries
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.service_delivery_id) *
            FROM contacts_dataset
            JOIN contact_ids ON contacts_dataset.id = contact_ids.contact_id
            ORDER BY contact_ids.service_delivery_id, contacts_dataset.is_primary DESC NULLS LAST
        )
        SELECT
            to_char(service_deliveries.interaction_date, 'DD/MM/YYYY') AS "Service Delivery Interaction",
            companies_dataset.name AS "Company Name",
            companies_dataset.company_number as "Companies House ID",
            companies_dataset.id AS "Data Hub Company ID",
            companies_dataset.cdms_reference_code AS "CDMS Reference Code",
            companies_dataset.address_postcode AS "Company Postcode",
            companies_dataset.company_number AS "Companies HouseID",
            companies_dataset.address_1 AS "Company Address Line 1",
            companies_dataset.address_2 AS "Company Address Line 2",
            companies_dataset.address_town AS "Company Address Town",
            companies_dataset.address_country AS "Company Address Country",
            companies_dataset.website AS "Company Website",
            companies_dataset.number_of_employees AS "Number of Employees",
            companies_dataset.is_number_of_employees_estimated AS "Number of Employees Estimated",
            companies_dataset.turnover AS "Turnover",
            companies_dataset.is_turnover_estimated AS "Turnover Estimated",
            companies_dataset.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            teams_dataset.name AS "DIT Team",
            companies_dataset.uk_region AS "Company UK Region",
            service_deliveries.service_delivery AS "Service Delivery",
            service_deliveries.interaction_subject AS "Subject",
            service_deliveries.interaction_notes AS "Notes",
            service_deliveries.net_company_receipt AS "Net Company Receipt",
            service_deliveries.grant_amount_offered AS "Grant Amount Offered",
            service_deliveries.service_delivery_status AS "Service Delivery Status",
            events_dataset.name AS "Event Name",
            events_dataset.event_type AS "Event Type",
            to_char(events_dataset.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            events_dataset.address_town AS "Event Town",
            events_dataset.address_country AS "Event Country",
            events_dataset.uk_region AS "Event UK Region",
            events_dataset.service_name AS "Event Service Name",
            teams_dataset.role AS "Team Role",
            to_char(service_deliveries.created_on, 'DD/MM/YYYY') AS "Created On Date"
        FROM service_deliveries
        JOIN companies_dataset ON service_deliveries.company_id = companies_dataset.id
        JOIN advisers_dataset ON service_deliveries.adviser_ids[1]::uuid = advisers_dataset.id
        JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
        LEFT JOIN events_dataset ON service_deliveries.event_id = events_dataset.id
        LEFT JOIN contacts ON contacts.service_delivery_id = service_deliveries.id
        ORDER BY service_deliveries.interaction_date
    '''


class ExportWinsYearlyViewPipeline(BaseViewPipeline):
    """Pipeline meta object for the yearly export wins report."""

    view_name = 'export_wins_yearly'
    dataset_pipeline = ExportWinsWinsDatasetPipeline
    start_date = datetime(2018, 1, 1)
    schedule_interval = '@yearly'
    query = '''
        SELECT
            "ID",
            "User",
            "Organisation or company name",
            "Data Hub (Companies House) or CDMS reference number",
            "Contact name",
            "Job title",
            "Contact email",
            "HQ location",
            "What kind of business deal was this win?",
            "Summarise the support provided to help achieve this win",
            "Overseas customer",
            "What are the goods or services?",
            "Date business won",
            "Country",
            "Total expected export value",
            "Total expected non export value",
            "Total expected odi value",
            "Does the expected value relate to",
            "Sector",
            "Prosperity Fund",
            "HVC code (if applicable)",
            "HVO Programme (if applicable)",
            "An HVO specialist was involved",
            "E-exporting programme",
            "type of support 1", 
            "type of support 2", 
            "type of support 3", 
            "associated programme 1", 
            "associated programme 2", 
            "associated programme 3", 
            "associated programme 4", 
            "associated programme 5",
            "I confirm that this information is complete and accurate",
            "My line manager has confirmed the decision to record this win",
            "Lead officer name",
            "Lead officer email address",
            "Secondary email address",
            "Line manager",
            "team type",
            "HQ team, Region or Post",
            "Medium-sized and high potential companies",
            "Export experience",
            "Created",
            "Audit",
            "Contributing advisors/team",
            "Customer email sent",
            "Customer email date",
            "Export breakdown 1",
            "Export breakdown 2",
            "Export breakdown 3",
            "Export breakdown 4",
            "Export breakdown 5",
            "Non-export breakdown 1",
            "Non-export breakdown 2",
            "Non-export breakdown 3",
            "Non-export breakdown 4",
            "Non-export breakdown 5",
            "Outward Direct Investment breakdown 1",
            "Outward Direct Investment breakdown 2",
            "Outward Direct Investment breakdown 3",
            "Outward Direct Investment breakdown 4",
            "Outward Direct Investment breakdown 5",
            "Customer response received",
            "Date response received",
            "Your name",
            "Please confirm these details are correct",
            "Other comments or changes to the win details",
            "Securing the win overall?",
            "Gaining access to contacts?",
            "Getting information or improved understanding of the country?",
            "Improving your profile or credibility in the country?",
            "Having confidence to explore or expand in the country?",
            "Developing or nurturing critical relationships?",
            "Overcoming a problem in the country (eg legal, regulatory, commercial)?",
            "The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)",
            "Our support was a prerequisite to generate this export value",
            "Our support helped you achieve this win more quickly",
            "What value do you estimate you would have achieved without our support?",
            "Apart from this win, when did your company last export goods or services?",
            "If you hadnt achieved this win, your company might have stopped exporting",
            "Apart from this win, you already have specific plans to export in the next 12 months",
            "It enabled you to expand into a new market",
            "It enabled you to increase exports as a proportion of your turnover",
            "It enabled you to maintain or expand in an existing market",
            "Would you be willing for DIT/Exporting is GREAT to feature your success in marketing materials?",
            "How did you first hear about DIT (or its predecessor, UKTI)",
            "Other marketing source"
        FROM (
            WITH export_wins AS (
                SELECT *
                FROM export_wins_wins_dataset
                WHERE export_wins_wins_dataset.customer_email_date IS NOT NULL
                AND date_trunc('year', export_wins_wins_dataset.confirmation_created) =  date_trunc('year', to_date('{{ ds }}', 'YYYY-MM-DD'))
            ), export_breakdowns AS (
                SELECT win_id, year, value
                FROM export_wins_breakdowns_dataset
                WHERE win_id IN (select id from export_wins)
                AND year >= EXTRACT(year FROM CURRENT_DATE)::int
                AND export_wins_breakdowns_dataset.type = 'Export'
            ), non_export_breakdowns AS (
                SELECT win_id, year, value 
                FROM export_wins_breakdowns_dataset
                WHERE win_id IN (select id from export_wins)
                AND year >= EXTRACT(year FROM CURRENT_DATE)::int
                AND export_wins_breakdowns_dataset.type = 'Non-export'
            ), odi_breakdowns AS (
                SELECT win_id, year, value
                FROM export_wins_breakdowns_dataset
                WHERE win_id IN (select id from export_wins)
                AND year >= EXTRACT(year FROM CURRENT_DATE)::int
                AND export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            ), contributing_advisers AS (
                SELECT win_id, STRING_AGG(CONCAT('Name: ', name, ', Team: ', team_type, ' - ', hq_team, ' - ', location), ', ') as advisers
                FROM export_wins_advisers_dataset
                GROUP  BY 1
            )
            SELECT
                CASE WHEN EXTRACT('month' FROM CURRENT_DATE)::int >= 4
                THEN (to_char(CURRENT_DATE, 'YYYY-04'))
                ELSE (to_char(CURRENT_DATE + interval '-1' year, 'YYYY-04'))
                END as current_financial_year,
                CASE WHEN EXTRACT('month' FROM export_wins.confirmation_created)::int >= 4
                THEN (to_char(export_wins.confirmation_created, 'YYYY-04'))
                ELSE (to_char(export_wins.confirmation_created + interval '-1' year, 'YYYY-04'))
                END as export_win_financial_year,
                export_wins.id AS "ID",
                CONCAT(export_wins.user_name, ' <', export_wins.user_email, '>') AS "User",
                export_wins.user_email AS "User email",
                export_wins.company_name AS "Organisation or company name",
                export_wins.cdms_reference AS "Data Hub (Companies House) or CDMS reference number",
                export_wins.customer_name AS "Contact name",
                export_wins.customer_job_title AS "Job title",
                export_wins.customer_email_address AS "Contact email",
                export_wins.customer_location AS "HQ location",
                export_wins.business_type AS "What kind of business deal was this win?",
                export_wins.description AS "Summarise the support provided to help achieve this win",
                export_wins.name_of_customer AS "Overseas customer",
                export_wins.name_of_export AS "What are the goods or services?",
                to_char(export_wins.date, 'DD/MM/YYYY') AS "Date business won",
                export_wins.country AS "Country",
                COALESCE(export_wins.total_expected_export_value, 0) AS "Total expected export value",
                COALESCE(export_wins.total_expected_non_export_value, 0) AS "Total expected non export value",
                COALESCE(export_wins.total_expected_odi_value, 0) AS "Total expected odi value",
                export_wins.goods_vs_services AS "Does the expected value relate to",
                export_wins.sector AS "Sector",
                COALESCE(export_wins.is_prosperity_fund_related, 'False') AS "Prosperity Fund",
                export_wins_hvc_dataset.name AS "HVC code (if applicable)",
                export_wins.hvo_programme AS "HVO Programme (if applicable)",
                COALESCE(export_wins.has_hvo_specialist_involvement, 'False') AS "An HVO specialist was involved",
                COALESCE(export_wins.is_e_exported, 'False') AS "E-exporting programme",
                export_wins.type_of_support_1 AS "type of support 1", 
                export_wins.type_of_support_2 AS "type of support 2", 
                export_wins.type_of_support_3 AS "type of support 3", 
                export_wins.associated_programme_1 AS "associated programme 1", 
                export_wins.associated_programme_2 AS "associated programme 2", 
                export_wins.associated_programme_3 AS "associated programme 3", 
                export_wins.associated_programme_4 AS "associated programme 4", 
                export_wins.associated_programme_5 AS "associated programme 5",
                export_wins.is_personally_confirmed AS "I confirm that this information is complete and accurate",
                export_wins.is_line_manager_confirmed AS "My line manager has confirmed the decision to record this win",
                export_wins.lead_officer_name AS "Lead officer name",
                export_wins.lead_officer_email_address AS "Lead officer email address",
                export_wins.other_official_email_address AS "Secondary email address",
                export_wins.line_manager_name AS "Line manager",
                export_wins.team_type AS "team type",
                export_wins.hq_team AS "HQ team, Region or Post",
                export_wins.business_potential AS "Medium-sized and high potential companies",
                export_wins.export_experience AS "Export experience",
                to_char(export_wins.created, 'DD/MM/YYYY') AS "Created",
                export_wins.audit AS "Audit",
                contributing_advisers.advisers AS "Contributing advisors/team",
                CASE WHEN export_wins.customer_email_date IS NOT NULL
                THEN 'Yes'
                ELSE 'No'
                END AS "Customer email sent",
                to_char(export_wins.customer_email_date, 'DD/MM/YYYY') AS "Customer email date",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, ': £', COALESCE(ebd1.value, 0)) AS "Export breakdown 1",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, ': £', COALESCE(ebd2.value, 0)) AS "Export breakdown 2",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, ': £', COALESCE(ebd3.value, 0)) "Export breakdown 3",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, ': £', COALESCE(ebd4.value, 0)) AS "Export breakdown 4",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, ': £', COALESCE(ebd5.value, 0)) AS "Export breakdown 5",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, ': £', COALESCE(nebd1.value, 0)) AS "Non-export breakdown 1",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, ': £', COALESCE(nebd2.value, 0)) AS "Non-export breakdown 2",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, ': £', COALESCE(nebd3.value, 0)) AS "Non-export breakdown 3",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, ': £', COALESCE(nebd4.value, 0)) AS "Non-export breakdown 4",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, ': £', COALESCE(nebd5.value, 0)) AS "Non-export breakdown 5",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, ': £', COALESCE(odibd1.value, 0)) AS "Outward Direct Investment breakdown 1",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, ': £', COALESCE(odibd2.value, 0)) AS "Outward Direct Investment breakdown 2",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, ': £', COALESCE(odibd3.value, 0)) AS "Outward Direct Investment breakdown 3",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, ': £', COALESCE(odibd4.value, 0)) AS "Outward Direct Investment breakdown 4",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, ': £', COALESCE(odibd5.value, 0)) AS "Outward Direct Investment breakdown 5",
                CASE WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'Yes'
                ELSE 'No'
                END AS "Customer response received",
                to_char(export_wins.confirmation_created, 'DD/MM/YYYY') AS "Date response received",
                export_wins.confirmation_name AS "Your name",
                export_wins.confirmation_agree_with_win  AS "Please confirm these details are correct",
                export_wins.confirmation_comments AS "Other comments or changes to the win details",
                export_wins.confirmation_our_support AS "Securing the win overall?",
                export_wins.confirmation_access_to_contacts AS "Gaining access to contacts?",
                export_wins.confirmation_access_to_information AS "Getting information or improved understanding of the country?",
                export_wins.confirmation_improved_profile AS "Improving your profile or credibility in the country?",
                export_wins.confirmation_gained_confidence AS "Having confidence to explore or expand in the country?",
                export_wins.confirmation_developed_relationships AS "Developing or nurturing critical relationships?",
                export_wins.confirmation_overcame_problem AS "Overcoming a problem in the country (eg legal, regulatory, commercial)?",
                export_wins.confirmation_involved_state_enterprise AS "The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)",
                COALESCE(export_wins.confirmation_interventions_were_prerequisite, 'False') AS "Our support was a prerequisite to generate this export value",
                COALESCE(export_wins.confirmation_support_improved_speed, 'False') AS "Our support helped you achieve this win more quickly",
                export_wins.confirmation_portion_without_help AS "What value do you estimate you would have achieved without our support?",
                export_wins.confirmation_last_export AS "Apart from this win, when did your company last export goods or services?",
                COALESCE(export_wins.confirmation_company_was_at_risk_of_not_exporting, 'False') AS "If you hadnt achieved this win, your company might have stopped exporting",
                COALESCE(export_wins.confirmation_has_explicit_export_plans, 'False') AS "Apart from this win, you already have specific plans to export in the next 12 months",
                COALESCE(export_wins.confirmation_has_enabled_expansion_into_new_market, 'False') AS "It enabled you to expand into a new market",
                COALESCE(export_wins.confirmation_has_increased_exports_as_percent_of_turnover, 'False') AS "It enabled you to increase exports as a proportion of your turnover",
                COALESCE(export_wins.confirmation_has_enabled_expansion_into_existing_market, 'False') AS "It enabled you to maintain or expand in an existing market",
                COALESCE(export_wins.confirmation_case_study_willing, 'False') AS "Would you be willing for DIT/Exporting is GREAT to feature your success in marketing materials?",
                export_wins.confirmation_marketing_source AS "How did you first hear about DIT (or its predecessor, UKTI)",
                export_wins.confirmation_other_marketing_source AS "Other marketing source"
            FROM export_wins
            -- Export breakdowns
            LEFT JOIN export_breakdowns ebd1 ON (
                export_wins.id = ebd1.win_id
                AND ebd1.year = extract(year FROM CURRENT_DATE)::int
            )
            LEFT JOIN export_breakdowns ebd2 ON (
                export_wins.id = ebd2.win_id
                AND ebd2.year = extract(year FROM CURRENT_DATE)::int + 1
            )
            LEFT JOIN export_breakdowns ebd3 ON (
                export_wins.id = ebd3.win_id
                AND ebd3.year = extract(year FROM CURRENT_DATE)::int + 2
            )
            LEFT JOIN export_breakdowns ebd4 ON (
                export_wins.id = ebd4.win_id
                AND ebd4.year = extract(year FROM CURRENT_DATE)::int + 3
            )
            LEFT JOIN export_breakdowns ebd5 ON (
                export_wins.id = ebd5.win_id
                AND ebd5.year = extract(year FROM CURRENT_DATE)::int + 4
            )
            -- Non export breakdowns
            LEFT JOIN non_export_breakdowns nebd1 ON (
                export_wins.id = nebd1.win_id
                AND nebd1.year = extract(year FROM CURRENT_DATE)::int
            )
            LEFT JOIN non_export_breakdowns nebd2 ON (
                export_wins.id = nebd2.win_id
                AND nebd2.year = extract(year FROM CURRENT_DATE)::int + 1
            )
            LEFT JOIN non_export_breakdowns nebd3 ON (
                export_wins.id = nebd3.win_id
                AND nebd3.year = extract(year FROM CURRENT_DATE)::int + 2
            )
            LEFT JOIN non_export_breakdowns nebd4 ON (
                export_wins.id = nebd4.win_id
                AND nebd4.year = extract(year FROM CURRENT_DATE)::int + 3
            )
            LEFT JOIN non_export_breakdowns nebd5 ON (
                export_wins.id = nebd5.win_id
                AND nebd5.year = extract(year FROM CURRENT_DATE)::int + 4
            )
            -- Outward Direct Investment breakdowns
            LEFT JOIN odi_breakdowns odibd1 ON (
                export_wins.id = odibd1.win_id
                AND odibd1.year = extract(year FROM CURRENT_DATE)::int
            )
            LEFT JOIN odi_breakdowns odibd2 ON (
                export_wins.id = odibd2.win_id
                AND odibd2.year = extract(year FROM CURRENT_DATE)::int + 1
            )
            LEFT JOIN odi_breakdowns odibd3 ON (
                export_wins.id = odibd3.win_id
                AND odibd3.year = extract(year FROM CURRENT_DATE)::int + 2
            )
            LEFT JOIN odi_breakdowns odibd4 ON (
                export_wins.id = odibd4.win_id
                AND odibd4.year = extract(year FROM CURRENT_DATE)::int + 3
            )
            LEFT JOIN odi_breakdowns odibd5 ON (
                export_wins.id = odibd5.win_id
                AND odibd5.year = extract(year FROM CURRENT_DATE)::int + 4
            )
            LEFT JOIN contributing_advisers ON contributing_advisers.win_id = export_wins.id
            LEFT JOIN export_wins_hvc_dataset ON export_wins.hvc = CONCAT(export_wins_hvc_dataset.campaign_id, export_wins_hvc_dataset.financial_year)
            ORDER BY export_wins.confirmation_created NULLS FIRST
        ) a
    '''


class FDIMonthlyStaticViewPipeline(BaseViewPipeline):
    """Static (materialized) monthly view of the FDI (investment projects) report"""

    view_name = 'data_hub_fdi_monthly_static'
    dataset_pipeline = InvestmentProjectsDatasetPipeline
    start_date = datetime(2020, 1, 1)
    materialized = True
    query = '''
        WITH fdi_report AS (
            WITH companies_last_version AS (
                SELECT *
                FROM companies_dataset
                    LEFT JOIN (
                        SELECT
                            DISTINCT ON (company_id)
                            company_id AS joined_id,
                            contact_name AS contact_name,
                            is_primary,
                            phone AS contact_phone,
                            email AS contact_email,
                            accepts_dit_email_marketing AS contact_accepts_dit_email_marketing
                        FROM contacts_dataset
                        ORDER BY company_id, is_primary DESC, modified_on DESC
                    ) contacts
                ON companies_dataset.id = contacts.joined_id
            )
            SELECT
                fdi.id AS unique_id,
                to_char(fdi.actual_land_date, 'YYYY-MM-DD') AS actual_land_date,
                to_char(fdi.estimated_land_date, 'YYYY-MM-DD') AS estimated_land_date,
                to_char(fdi.created_on, 'YYYY-MM-DD') AS created_on,
                to_char(fdi.modified_on, 'YYYY-MM-DD') AS modified_on,
                SUBSTRING(i.date_of_latest_interaction, 0, 11) AS date_of_latest_interaction,
                fdi.name,
                fdi.description,
                fdi.project_reference,
                fdi.total_investment,
                fdi.number_new_jobs,
                fdi.number_safeguarded_jobs,
                fdi.client_requirements,
                fdi.address_1,
                fdi.address_2,
                fdi.address_postcode,
                fdi.id,
                fdi.client_relationship_manager_id,
                crm.first_name || ' ' || crm.last_name AS client_relationship_manager_name,
                crmt.name AS client_relationship_manager_team,
                acm.first_name || ' ' || acm.last_name AS account_manager_name,
                acmt.name AS account_manager_team,
                fdi.fdi_type,
                fdi.investment_type,
                fdi.investor_company_id,
                inv.name AS investor_company_name,
                inv.company_number AS investor_company_comp_house_id,
                inv.headquarter_type AS investor_company_headquarter_type,
                inv.classification AS investor_company_company_tier,
                inv.sector AS investor_company_sector,
                inv.address_1 AS investor_company_address_1,
                inv.address_2 AS investor_company_address_2,
                inv.uk_region AS investor_company_uk_region,
                inv.address_country AS investor_company_country,
                inv.contact_name AS investor_company_contact_name,
                inv.is_primary AS investor_company_contact_marked_as_primary_contact,
                inv.contact_phone AS investor_company_contact_phone,
                inv.contact_email AS investor_company_contact_email,
                inv.contact_accepts_dit_email_marketing AS investor_company_contact_accepts_dit_email_marketing,
                inv.one_list_account_owner_id,
                fdi.specific_programme,
                fdi.stage,
                paa.first_name || ' ' || paa.last_name AS project_assurance_adviser_name,
                paat.name AS project_assurance_adviser_team,
                pm.first_name || ' ' || pm.last_name AS project_manager_name,
                pmt.name AS project_manager_team,
                fdi.sector,
                fdi.uk_company_id,
                ukc.name AS uk_company_name,
                ukc.company_number AS uk_company_comp_house_id,
                ukc.sector AS uk_company_sector,
                ukc.address_1 AS uk_company_address_1,
                ukc.address_2 AS uk_company_address_2,
                ukc.address_postcode AS uk_company_postcode,
                ukc.uk_region AS uk_company_uk_region,
                ukc.address_country AS uk_company_country,
                ukc.contact_name AS uk_company_contact_name,
                ukc.is_primary AS uk_company_contact_marked_as_primary_contact,
                ukc.contact_phone AS uk_company_contact_phone,
                ukc.contact_email AS uk_company_contact_email,
                ukc.contact_accepts_dit_email_marketing AS uk_company_contact_accepts_dit_email_marketing,
                fdi.likelihood_to_land,
                fdi.fdi_value,
                cre.first_name || ' ' || cre.last_name AS created_by_name,
                cret.name AS created_by_team,
                mod.first_name || ' ' || mod.last_name AS modified_by_name,
                modt.name as modified_by_team,
                fdi.status,
                fdi.anonymous_description,
                fdi.associated_non_fdi_r_and_d_project_id,
                array_to_string(fdi.competing_countries, '; ') as competing_countries,
                (
                    SELECT STRING_AGG(CONCAT(advisers_dataset.first_name, ' ', advisers_dataset.last_name, ' (', teams_dataset.name, ')'), '; ')
                    FROM advisers_dataset
                    JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
                    WHERE advisers_dataset.id = ANY(fdi.team_member_ids::uuid[])
                ) AS team_members,
                fdi.investor_type,
                fdi.level_of_involvement,
                fdi.foreign_equity_investment,
                fdi.government_assistance,
                fdi.r_and_d_budget,
                fdi.non_fdi_r_and_d_budget,
                fdi.new_tech_to_uk,
                fdi.average_salary,
                fdi.referral_source_activity,
                fdi.referral_source_activity_website,
                fdi.referral_source_activity_marketing,
                fdi.delivery_partners,
                fdi.possible_uk_regions,
                fdi.actual_uk_regions,
                CASE
                  WHEN fdi.other_business_activity IS NULL
                       AND fdi.business_activities IS NOT NULL
                    THEN fdi.business_activities
                  WHEN fdi.other_business_activity IS NOT NULL
                       AND fdi.business_activities IS NULL
                    THEN fdi.other_business_activity
                  WHEN fdi.other_business_activity IS NOT NULL
                       AND fdi.business_activities IS NOT NULL
                    THEN fdi.other_business_activity || ', ' || fdi.business_activities
                  ELSE NULL END AS business_activities,
                fdi.project_arrived_in_triage_on,
                fdi.proposal_deadline,
                CASE WHEN fdi.export_revenue THEN 'yes' ELSE 'no' END AS export_revenue,
                fdi.strategic_drivers,
                fdi.gross_value_added,
                fdi.gva_multiplier
            FROM investment_projects_dataset fdi
             LEFT JOIN companies_last_version inv ON fdi.investor_company_id = inv.id
             LEFT JOIN companies_last_version ukc ON fdi.uk_company_id = ukc.id
             LEFT JOIN advisers_dataset crm ON fdi.client_relationship_manager_id = crm.id
             LEFT JOIN teams_dataset crmt ON crm.team_id = crmt.id
             LEFT JOIN advisers_dataset paa ON fdi.project_assurance_adviser_id = paa.id
             LEFT JOIN teams_dataset paat ON paa.team_id = paat.id
             LEFT JOIN advisers_dataset pm ON fdi.project_manager_id = pm.id
             LEFT JOIN teams_dataset pmt ON pm.team_id = pmt.id
             LEFT JOIN advisers_dataset cre ON fdi.created_by_id = cre.id
             LEFT JOIN teams_dataset cret ON cre.team_id = cret.id
             LEFT JOIN advisers_dataset mod ON fdi.modified_by_id = mod.id
             LEFT JOIN teams_dataset modt ON mod.team_id = modt.id
             LEFT JOIN advisers_dataset acm ON inv.one_list_account_owner_id = acm.id
             LEFT JOIN teams_dataset acmt ON acm.team_id = acmt.id
             LEFT JOIN (
                SELECT investment_project_id, max(interaction_date)::text as date_of_latest_interaction
                FROM interactions_dataset i
                WHERE investment_project_id IS NOT NULL
                GROUP BY investment_project_id
             ) i ON fdi.id = i.investment_project_id
            WHERE (
                (fdi.actual_land_date >= '2018-04-01' OR fdi.estimated_land_date >= '2018-04-01')
                AND LOWER(fdi.status) IN ('ongoing', 'won')
            )
            OR (
                (fdi.modified_on BETWEEN (date_trunc('month', to_date('{{ ds }}', 'YYYY-MM-DD')) - interval '1 year') and now())
                AND LOWER(fdi.status) NOT IN ('ongoing', 'won')
            )
            ORDER BY fdi.actual_land_date, fdi.estimated_land_date ASC
        )
        SELECT DISTINCT ON (unique_id)
            actual_land_date, estimated_land_date, created_on, modified_on,
            date_of_latest_interaction, name, description,  project_reference,  total_investment,
            number_new_jobs, number_safeguarded_jobs, client_requirements, address_1, address_2,
            address_postcode, id, client_relationship_manager_id, client_relationship_manager_name,
            client_relationship_manager_team, account_manager_name, account_manager_team, fdi_type,
            investment_type, investor_company_id, investor_company_name,
            investor_company_comp_house_id, investor_company_headquarter_type,
            investor_company_company_tier, investor_company_sector, investor_company_address_1,
            investor_company_address_2, investor_company_uk_region, investor_company_country,
            investor_company_contact_name, investor_company_contact_marked_as_primary_contact,
            investor_company_contact_phone, investor_company_contact_email,
            investor_company_contact_accepts_dit_email_marketing, specific_programme, stage,
            project_assurance_adviser_name, project_assurance_adviser_team, project_manager_name,
            project_manager_team, sector, uk_company_id, uk_company_name, uk_company_comp_house_id,
            uk_company_sector, uk_company_address_1, uk_company_address_2, uk_company_postcode,
            uk_company_uk_region, uk_company_country, uk_company_contact_name,
            uk_company_contact_marked_as_primary_contact, uk_company_contact_phone,
            uk_company_contact_email, uk_company_contact_accepts_dit_email_marketing,
            likelihood_to_land, fdi_value, created_by_name, created_by_team, modified_by_name,
            modified_by_team, status, anonymous_description, associated_non_fdi_r_and_d_project_id,
            competing_countries, team_members, investor_type, level_of_involvement,
            foreign_equity_investment, government_assistance, r_and_d_budget,
            non_fdi_r_and_d_budget, new_tech_to_uk, average_salary, referral_source_activity,
            referral_source_activity_website, referral_source_activity_marketing, delivery_partners,
            possible_uk_regions, actual_uk_regions, business_activities, project_arrived_in_triage_on,
            proposal_deadline, export_revenue, strategic_drivers, gross_value_added, gva_multiplier
        FROM fdi_report f
    '''


for pipeline in BaseViewPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline.get_dag()
