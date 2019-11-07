"""A module that defines Airflow DAGS for dataset pipelines."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import constants
from dataflow.operators.dataset import (
    run_fetch,
    create_tables,
    execute_insert_into,
    insert_from_temporary_table,
)


class BaseDatasetPipeline:
    target_db = 'datasets_db'
    start_date = datetime(2019, 11, 5)
    end_date = None
    schedule_interval = '@daily'

    @classmethod
    def get_dag(pipeline):
        run_fetch_task_id = f'RunFetch{pipeline.__name__}'

        dag = DAG(
            pipeline.__name__,
            catchup=False,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
            },
            start_date=pipeline.start_date,
            end_date=pipeline.end_date,
            schedule_interval=pipeline.schedule_interval,
            max_active_runs=1,
            user_defined_macros={
                'table_name': pipeline.table_name,
                'field_mapping': pipeline.field_mapping,
                'run_fetch_task_id': run_fetch_task_id,
            },
        )

        t1 = PythonOperator(
            task_id=run_fetch_task_id,
            python_callable=run_fetch,
            provide_context=True,
            op_args=[f'{pipeline.source_url}'],
        )

        t3 = PythonOperator(
            task_id='create-tables',
            python_callable=create_tables,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        insert_group = []
        for index in range(constants.INGEST_TASK_CONCURRENCY):
            insert_group.append(
                PythonOperator(
                    task_id=f'execute-insert-into-{index}',
                    python_callable=execute_insert_into,
                    provide_context=True,
                    op_args=[f'{pipeline.target_db}'],
                )
            )

        tend = PythonOperator(
            task_id='insert-from-temporary-table',
            python_callable=insert_from_temporary_table,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        dag << t1
        dag << tend << insert_group << t3

        return dag


class OMISDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for OMISDataset."""

    table_name = 'omis_dataset'
    source_url = '{0}/v4/dataset/omis-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('cancellation_reason__name', 'cancellation_reason', 'text'),
        ('cancelled_on', 'cancelled_date', 'timestamp with time zone'),
        ('company_id', 'company_id', 'uuid'),
        ('completed_on', 'completion_date', 'timestamp with time zone'),
        ('contact_id', 'contact_id', 'uuid'),
        ('created_by__dit_team_id', 'dit_team_id', 'uuid'),
        ('created_on', 'created_date', 'timestamp with time zone'),
        ('delivery_date', 'delivery_date', 'date'),
        ('id', 'id', 'uuid primary key'),
        ('invoice__subtotal_cost', 'subtotal', 'integer'),
        ('paid_on', 'payment_received_date', 'timestamp with time zone'),
        ('primary_market__name', 'market', 'text'),
        ('reference', 'omis_order_reference', 'character varying(100)'),
        ('sector_name', 'sector', 'character varying(255)'),
        ('services', 'services', 'text'),
        ('status', 'order_status', 'character varying(100)'),
        ('subtotal_cost', 'net_price', 'integer'),
        ('uk_region__name', 'uk_region', 'text'),
    ]


class InvestmentProjectsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for InvestmentProjectsDataset."""

    table_name = 'investment_projects_dataset'
    source_url = '{0}/v4/dataset/investment-projects-dataset'.format(
        constants.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('actual_land_date', 'actual_land_date', 'date'),
        ('actual_uk_region_names', 'actual_uk_regions', 'text'),
        ('allow_blank_possible_uk_regions', 'possible_uk_regions', 'boolean'),
        ('anonymous_description', 'anonymous_description', 'text'),
        (
            'associated_non_fdi_r_and_d_project__id',
            'associated_non_fdi_r_and_d_project_id',
            'character varying(100)',
        ),
        ('average_salary__name', 'average_salary', 'text'),
        ('business_activity_names', 'business_activities', 'text'),
        ('client_relationship_manager_id', 'client_relationship_manager_id', 'uuid'),
        ('clint_requirements', 'client_requirements', 'text'),
        ('competing_countries', 'competing_countries', 'text'),
        ('created_by_id', 'created_by_id', 'uuid'),
        ('created_on', 'created_on', 'timestamp with time zone'),
        ('delivery_partner_names', 'delivery_partners', 'text'),
        ('description', 'description', 'text'),
        ('estimated_land_date', 'estimated_land_date', 'date'),
        ('export_revenue', 'export_revenue', 'boolean'),
        ('fdi_type__name', 'fdi_type', 'text'),
        ('fdi_value__name', 'fdi_value', 'text'),
        ('foreign_equity_investment', 'foreign_equity_investment', 'decimal'),
        ('government_assistance', 'government_assistance', 'boolean'),
        ('gross_value_added', 'gross_value_added', 'decimal'),
        ('gva_multiplier__multiplier', 'gva_multiplier', 'decimal'),
        ('id', 'id', 'uuid primary key'),
        ('investment_type__name', 'investment_type', 'text'),
        ('investor_company_id', 'investor_company_id', 'uuid'),
        (
            'investor_company_sector',
            'investor_company_sector',
            'character varying(255)',
        ),
        ('investor_type__name', 'investor_type', 'text'),
        ('level_of_involvement_name', 'level_of_involvement', 'text'),
        ('likelihood_to_land__name', 'likelihood_to_land', 'text'),
        ('modified_by_id', 'modified_by_id', 'uuid'),
        ('modified_on', 'modified_on', 'timestamp with time zone'),
        ('name', 'name', 'character varying(255) NOT NULL'),
        ('new_tech_to_uk', 'new_tech_to_uk', 'boolean'),
        ('non_fdi_r_and_d_budget', 'non_fdi_r_and_d_budget', 'boolean'),
        ('number_new_jobs', 'number_new_jobs', 'integer'),
        ('number_safeguarded_jobs', 'number_safeguarded_jobs', 'integer'),
        ('project_arrived_in_triage_on', 'project_arrived_in_triage_on', 'date'),
        ('project_assurance_adviser_id', 'project_assurance_adviser_id', 'uuid'),
        ('project_manager_id', 'project_manager_id', 'uuid'),
        ('project_reference', 'project_reference', 'text'),
        ('proposal_deadline', 'proposal_deadline', 'date'),
        ('r_and_d_budget', 'r_and_d_budget', 'boolean'),
        ('referral_source_activity__name', 'referral_source_activity', 'text'),
        (
            'referral_source_activity_marketing__name',
            'referral_source_activity_marketing',
            'text',
        ),
        (
            'referral_source_activity_website__name',
            'referral_source_activity_website',
            'text',
        ),
        ('sector_name', 'sector', 'character varying(255)'),
        ('specific_programme__name', 'specific_programme', 'text'),
        ('stage__name', 'stage', 'text NOT NULL'),
        ('status', 'status', 'character varying(255)'),
        ('strategic_driver_names', 'strategic_drivers', 'text'),
        ('team_member_ids', 'team_member_ids', 'text []'),
        ('total_investment', 'total_investment', 'decimal'),
        ('uk_company_id', 'uk_company_id', 'uuid'),
        ('uk_company_sector', 'uk_company_sector', 'character varying(255)'),
    ]


class InteractionsDatasetPipeline(BaseDatasetPipeline):
    table_name = 'interactions_dataset'
    source_url = '{}/v4/dataset/interactions-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('adviser_ids', 'adviser_ids', 'text []'),
        (
            'communication_channel__name',
            'communication_channel',
            'character varying(255)',
        ),
        ('company_id', 'company_id', 'uuid'),
        ('contact_ids', 'contact_ids', 'text []'),
        ('created_on', 'created_on', 'timestamp with time zone'),
        ('date', 'interaction_date', 'date'),
        ('event_id', 'event_id', 'uuid'),
        ('grant_amount_offered', 'grant_amount_offered', 'decimal'),
        ('id', 'id', 'uuid primary key'),
        ('interaction_link', 'interaction_link', 'character varying(255)'),
        ('investment_project_id', 'investment_project_id', 'uuid'),
        ('kind', 'interaction_kind', 'character varying(255)'),
        ('net_company_receipt', 'net_company_receipt', 'decimal'),
        ('notes', 'interaction_notes', 'text'),
        ('sector', 'sector', 'character varying(255)'),
        (
            'service_delivery_status__name',
            'service_delivery_status',
            'character varying(255)',
        ),
        ('service_delivery', 'service_delivery', 'character varying(255)'),
        ('subject', 'interaction_subject', 'text'),
    ]


class ContactsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for ContactsDataset."""

    table_name = 'contacts_dataset'
    source_url = '{0}/v4/dataset/contacts-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('accepts_dit_email_marketing', 'accepts_dit_email_marketing', 'boolean'),
        ('address_1', 'address_1', 'character varying(255)'),
        ('address_2', 'address_2', 'character varying(255)'),
        ('address_country__name', 'address_country', 'text'),
        ('address_county', 'address_county', 'character varying(255)'),
        ('address_postcode', 'address_postcode', 'character varying(255)'),
        ('address_same_as_company', 'address_same_as_company', 'boolean'),
        ('address_town', 'address_town', 'character varying(255)'),
        ('company_id', 'company_id', 'uuid'),
        ('created_on', 'date_added_to_datahub', 'date'),
        ('email', 'email', 'character varying(255)'),
        ('email_alternative', 'email_alternative', 'character varying(255)'),
        ('id', 'id', 'uuid primary key'),
        ('job_title', 'job_title', 'character varying(255)'),
        ('name', 'contact_name', 'text'),
        ('notes', 'notes', 'text'),
        ('primary', 'is_primary', 'boolean'),
        ('telephone_alternative', 'telephone_alternative', 'character varying(255)'),
        ('telephone_number', 'phone', 'character varying(255)'),
    ]


class CompaniesDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for CompaniesDataset."""

    table_name = 'companies_dataset'
    source_url = '{0}/v4/dataset/companies-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('address_1', 'address_1', 'character varying(255)'),
        ('address_2', 'address_2', 'character varying(255)'),
        ('address_county', 'address_county', 'character varying(255)'),
        ('address_country__name', 'address_country', 'character varying(255)'),
        ('address_postcode', 'address_postcode', 'character varying(255)'),
        ('address_town', 'address_town', 'character varying(255)'),
        ('business_type__name', 'business_type', 'character varying(255)'),
        ('company_number', 'company_number', 'character varying(255)'),
        ('created_on', 'created_on', 'date'),
        ('description', 'description', 'text'),
        ('duns_number', 'duns_number', 'character varying(9)'),
        (
            'export_experience_category__name',
            'export_experience',
            'character varying(255)',
        ),
        ('id', 'id', 'uuid primary key'),
        (
            'is_number_of_employees_estimated',
            'is_number_of_employees_estimated',
            'boolean',
        ),
        ('is_turnover_estimated', 'is_turnover_estimated', 'boolean'),
        ('name', 'name', 'character varying(255)'),
        ('number_of_employees', 'number_of_employees', 'integer'),
        ('one_list_tier__name', 'classification', 'character varying(255)'),
        ('reference_code', 'cdms_reference_code', 'character varying(255)'),
        ('registered_address_1', 'registered_address_1', 'character varying(255)'),
        ('registered_address_2', 'registered_address_2', 'character varying(255)'),
        (
            'registered_address_country__name',
            'registered_address_country',
            'character varying(255)',
        ),
        (
            'registered_address_county',
            'registered_address_county',
            'character varying(255)',
        ),
        (
            'registered_address_postcode',
            'registered_address_postcode',
            'character varying(255)',
        ),
        (
            'registered_address_town',
            'registered_address_town',
            'character varying(255)',
        ),
        ('sector_name', 'sector', 'character varying(255)'),
        ('trading_names', 'trading_names', 'character varying(255)'),
        ('turnover', 'turnover', 'bigint'),
        ('uk_region__name', 'uk_region', 'character varying(255)'),
        ('vat_number', 'vat_number', 'character varying(255)'),
        ('website', 'website', 'character varying(255)'),
    ]


class AdvisersDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for AdvisersDataset."""

    table_name = 'advisers_dataset'
    source_url = '{0}/v4/dataset/advisers-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('id', 'id', 'uuid primary key'),
        ('date_joined', 'date_joined', 'date'),
        ('first_name', 'first_name', 'character varying(255)'),
        ('last_name', 'last_name', 'character varying(255)'),
        ('telephone_number', 'telephone_number', 'character varying(255)'),
        ('contact_email', 'contact_email', 'character varying(255)'),
        ('dit_team_id', 'team_id', 'uuid'),
        ('is_active', 'is_active', 'boolean'),
    ]


class TeamsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for TeamsDataset."""

    table_name = 'teams_dataset'
    source_url = '{0}/v4/dataset/teams-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('id', 'id', 'uuid primary key'),
        ('name', 'name', 'character varying(255)'),
        ('role__name', 'role', 'character varying(255)'),
        ('uk_region__name', 'uk_region', 'character varying(255)'),
        ('country__name', 'country', 'character varying(255)'),
    ]


class EventsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for EventsDataset."""

    table_name = 'events_dataset'
    source_url = '{0}/v4/dataset/events-dataset'.format(constants.DATAHUB_BASE_URL)
    field_mapping = [
        ('address_1', 'address_1', 'character varying(255)'),
        ('address_2', 'address_2', 'character varying(255)'),
        ('address_country__name', 'address_country', 'character varying(255)'),
        ('address_county', 'address_county', 'character varying(255)'),
        ('address_postcode', 'address_postcode', 'character varying(255)'),
        ('address_town', 'address_town', 'character varying(255)'),
        ('created_on', 'created_on', 'timestamp with time zone'),
        ('end_date', 'end_date', 'date'),
        ('event_type__name', 'event_type', 'character varying(255)'),
        ('id', 'id', 'uuid primary key'),
        ('lead_team_id', 'lead_team_id', 'uuid'),
        ('location_type__name', 'location_type', 'character varying(255)'),
        ('name', 'name', 'character varying(255)'),
        ('notes', 'notes', 'text'),
        ('organiser_id', 'organiser_id', 'uuid'),
        ('service_name', 'service_name', 'character varying(255)'),
        ('start_date', 'start_date', 'date'),
        ('team_ids', 'team_ids', 'text []'),
        ('uk_region__name', 'uk_region', 'character varying(255)'),
    ]


for pipeline in BaseDatasetPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline.get_dag()
