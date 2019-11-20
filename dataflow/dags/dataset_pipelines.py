"""A module that defines Airflow DAGS for dataset pipelines."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import config
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
                'priority_weight': 1,
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

        fetch_task = PythonOperator(
            task_id=run_fetch_task_id,
            python_callable=run_fetch,
            provide_context=True,
            op_args=[f'{pipeline.source_url}'],
            priority_weight=2,
        )

        create_tables_task = PythonOperator(
            task_id='create-tables',
            python_callable=create_tables,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        insert_task_group = []
        for index in range(config.INGEST_TASK_CONCURRENCY):
            insert_task_group.append(
                PythonOperator(
                    task_id=f'execute-insert-into-{index}',
                    python_callable=execute_insert_into,
                    provide_context=True,
                    op_args=[f'{pipeline.target_db}'],
                )
            )

        insert_from_temp_task = PythonOperator(
            task_id='insert-from-temporary-table',
            python_callable=insert_from_temporary_table,
            provide_context=True,
            op_args=[f'{pipeline.target_db}'],
        )

        dag << fetch_task
        dag << insert_from_temp_task << insert_task_group << create_tables_task

        return dag


class CompanyExportToCountries(BaseDatasetPipeline):

    table_name = 'company_export_to_countries_dataset'
    source_url = '{0}/v4/dataset/company-export-to-countries-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('id', 'id', 'integer primary key'),
        ('company_id', 'company_id', 'uuid'),
        ('country__name', 'country', 'character varying(255)'),
        ('country__iso_alpha2_code', 'country_iso_alpha2_code', 'character varying(2)'),
    ]


class CompanyFutureInterestCountries(BaseDatasetPipeline):

    table_name = 'company_future_interest_countries_dataset'
    source_url = '{0}/v4/dataset/company-future-interest-countries-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('id', 'id', 'integer primary key'),
        ('company_id', 'company_id', 'uuid'),
        ('country__name', 'country', 'character varying(255)'),
        ('country__iso_alpha2_code', 'country_iso_alpha2_code', 'character varying(2)'),
    ]


class CountriesOfInterestServiceCountriesAndSectorsOfInterest(BaseDatasetPipeline):

    table_name = 'countries_of_interest_service_countries_and_sectors_of_interest'
    source_url = (
        '{0}/api/v1/get-company-countries-and-sectors-of-interest'
        '?orientation=records'.format(config.COUNTRIES_OF_INTEREST_BASE_URL)
    )
    field_mapping = [
        ('companyId', 'company_id', 'character varying(100)'),
        ('countryOfInterest', 'country_of_interest', 'character varying(2)'),
        ('sectorOfInterest', 'sector_of_interest', 'character varying(50)'),
        ('source', 'source', 'character varying(50)'),
        ('sourceId', 'source_id', 'character varying(100)'),
        ('timestamp', 'timestamp', 'timestamp with time zone'),
    ]


class CountriesOfInterestServiceCountriesOfInterest(BaseDatasetPipeline):

    table_name = 'countries_of_interest_service_countries_of_interest'
    source_url = (
        '{0}/api/v1/get-company-countries-of-interest'
        '?orientation=records'.format(config.COUNTRIES_OF_INTEREST_BASE_URL)
    )
    field_mapping = [
        ('company_id', 'character varying(100)'),
        ('country_of_interest', 'character varying(2)'),
        ('source', 'character varying(50)'),
        ('source_id', 'character varying(100)'),
        ('timestamp', 'timestamp with time zone'),
    ]


class CountriesOfInterestServiceExportCountries(BaseDatasetPipeline):

    table_name = 'countries_of_interest_service_export_countries'
    source_url = (
        '{0}/api/v1/get-company-export-countries'
        '?orientation=records'.format(config.COUNTRIES_OF_INTEREST_BASE_URL)
    )
    field_mapping = [
        ('companyId', 'company_id', 'character varying(100)'),
        ('exportCountry', 'export_country', 'character varying(2)'),
        ('source', 'source', 'character varying(50)'),
        ('sourceId', 'source_id', 'character varying(100)'),
        ('timestamp', 'timestamp', 'timestamp with time zone'),
    ]


class CountriesOfInterestServiceSectorsOfInterest(BaseDatasetPipeline):

    table_name = 'countries_of_interest_service_sectors_of_interest'
    source_url = (
        '{0}/api/v1/get-company-sectors-of-interest'
        '?orientation=records'.format(config.COUNTRIES_OF_INTEREST_BASE_URL)
    )
    field_mapping = [
        ('companyId', 'company_id', 'character varying(100)'),
        ('sectorOfInterest', 'sector_of_interest', 'character varying(50)'),
        ('source', 'source', 'character varying(50)'),
        ('sourceId', 'source_id', 'character varying(100)'),
        ('timestamp', 'timestamp', 'timestamp with time zone'),
    ]


class OMISDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for OMISDataset."""

    table_name = 'omis_dataset'
    source_url = '{0}/v4/dataset/omis-dataset'.format(config.DATAHUB_BASE_URL)
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
        config.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('actual_land_date', 'actual_land_date', 'date'),
        ('actual_uk_region_names', 'actual_uk_regions', 'text'),
        ('address_1', 'address_1', 'character varying(255)'),
        ('address_2', 'address_2', 'character varying(255)'),
        ('address_town', 'address_town', 'character varying(255)'),
        ('address_postcode', 'address_postcode', 'character varying(255)'),
        ('allow_blank_possible_uk_regions', 'possible_uk_regions', 'boolean'),
        ('anonymous_description', 'anonymous_description', 'text'),
        ('archived', 'archived', 'boolean'),
        ('archived_by', 'archived_by', 'uuid'),
        ('archived_on', 'archived_on', 'timestamp with time zone'),
        ('archived_reason', 'archived_reason', 'character varying(255)'),
        (
            'associated_non_fdi_r_and_d_project__id',
            'associated_non_fdi_r_and_d_project_id',
            'character varying(100)',
        ),
        ('average_salary__name', 'average_salary', 'text'),
        ('business_activity_names', 'business_activities', 'text'),
        ('client_relationship_manager_id', 'client_relationship_manager_id', 'uuid'),
        ('client_requirements', 'client_requirements', 'text'),
        ('competing_countries', 'competing_countries', 'text'),
        ('country_lost_to__name', 'country_lost_to', 'character varying(255)'),
        ('created_by_id', 'created_by_id', 'uuid'),
        ('created_on', 'created_on', 'timestamp with time zone'),
        ('date_abandoned', 'date_abandoned', 'date'),
        ('date_lost', 'date_lost', 'date'),
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
        (
            'other_business_activity',
            'other_business_activity',
            'character varying(255)',
        ),
        ('some_new_jobs', 'some_new_jobs', 'boolean'),
        ('project_arrived_in_triage_on', 'project_arrived_in_triage_on', 'date'),
        ('project_assurance_adviser_id', 'project_assurance_adviser_id', 'uuid'),
        ('project_manager_id', 'project_manager_id', 'uuid'),
        ('project_reference', 'project_reference', 'text'),
        ('proposal_deadline', 'proposal_deadline', 'date'),
        ('quotable_as_public_case_study', 'quotable_as_public_case_study', 'boolean'),
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
        ('uk_company_decided', 'uk_company_decided', 'boolean'),
        ('uk_company_id', 'uk_company_id', 'uuid'),
        ('uk_company_sector', 'uk_company_sector', 'character varying(255)'),
    ]


class InteractionsDatasetPipeline(BaseDatasetPipeline):
    table_name = 'interactions_dataset'
    source_url = '{}/v4/dataset/interactions-dataset'.format(config.DATAHUB_BASE_URL)
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
        ('theme', 'theme', 'character varying(255)'),
    ]


class ContactsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for ContactsDataset."""

    table_name = 'contacts_dataset'
    source_url = '{0}/v4/dataset/contacts-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('accepts_dit_email_marketing', 'accepts_dit_email_marketing', 'boolean'),
        ('address_1', 'address_1', 'character varying(255)'),
        ('address_2', 'address_2', 'character varying(255)'),
        ('address_country__name', 'address_country', 'text'),
        ('address_county', 'address_county', 'character varying(255)'),
        ('address_postcode', 'address_postcode', 'character varying(255)'),
        ('address_same_as_company', 'address_same_as_company', 'boolean'),
        ('address_town', 'address_town', 'character varying(255)'),
        ('archived' 'archived', 'boolean'),
        ('archived_on', 'archived_on', 'timestamp with time zone'),
        ('company_id', 'company_id', 'uuid'),
        ('created_on', 'date_added_to_datahub', 'date'),
        ('email', 'email', 'character varying(255)'),
        ('email_alternative', 'email_alternative', 'character varying(255)'),
        ('id', 'id', 'uuid primary key'),
        ('job_title', 'job_title', 'character varying(255)'),
        ('modified_on', 'modified_on', 'timestamp with time zone'),
        ('name', 'contact_name', 'text'),
        ('notes', 'notes', 'text'),
        ('primary', 'is_primary', 'boolean'),
        ('telephone_alternative', 'telephone_alternative', 'character varying(255)'),
        ('telephone_number', 'phone', 'character varying(255)'),
    ]


class CompaniesDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for CompaniesDataset."""

    table_name = 'companies_dataset'
    source_url = '{0}/v4/dataset/companies-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('address_1', 'address_1', 'character varying(255)'),
        ('address_2', 'address_2', 'character varying(255)'),
        ('address_county', 'address_county', 'character varying(255)'),
        ('address_country__name', 'address_country', 'character varying(255)'),
        ('address_postcode', 'address_postcode', 'character varying(255)'),
        ('address_town', 'address_town', 'character varying(255)'),
        ('archived', 'archived', 'boolean'),
        ('archived_on', 'archived_on', 'timestamp with time zone'),
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
        ('headquarter_type__name', 'headquarter_type', 'character varying(255)'),
        ('id', 'id', 'uuid primary key'),
        (
            'is_number_of_employees_estimated',
            'is_number_of_employees_estimated',
            'boolean',
        ),
        ('is_turnover_estimated', 'is_turnover_estimated', 'boolean'),
        ('modified_on', 'modified_on', 'timestamp with time zone'),
        ('name', 'name', 'character varying(255)'),
        ('number_of_employees', 'number_of_employees', 'integer'),
        ('one_list_account_owner_id', 'one_list_account_owner_id', 'uuid'),
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
    source_url = '{0}/v4/dataset/advisers-dataset'.format(config.DATAHUB_BASE_URL)
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
    source_url = '{0}/v4/dataset/teams-dataset'.format(config.DATAHUB_BASE_URL)
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
    source_url = '{0}/v4/dataset/events-dataset'.format(config.DATAHUB_BASE_URL)
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


class ExportWinsAdvisersDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins advisers."""

    table_name = 'export_wins_advisers_dataset'
    source_url = '{0}/datasets/advisors'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        ('hq_team_display', 'hq_team', 'character varying(255)'),
        ('id', 'id', 'integer primary key'),
        ('location', 'location', 'character varying(255)'),
        ('name', 'name', 'character varying(255)'),
        ('team_type_display', 'team_type', 'character varying(255)'),
        ('win__id', 'win_id', 'uuid'),
    ]


class ExportWinsBreakdownsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins yearly export/non-export value."""

    table_name = 'export_wins_breakdowns_dataset'
    source_url = '{0}/datasets/breakdowns'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        ('id', 'id', 'integer primary key'),
        ('win__id', 'win_id', 'uuid'),
        ('breakdown_type', 'type', 'character varying(255)'),
        ('year', 'year', 'integer'),
        ('value', 'value', 'bigint'),
    ]


class ExportWinsWinsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins wins."""

    table_name = 'export_wins_wins_dataset'
    source_url = '{0}/datasets/wins'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        (
            'associated_programme_1_display',
            'associated_programme_1',
            'character varying(255)',
        ),
        (
            'associated_programme_2_display',
            'associated_programme_2',
            'character varying(255)',
        ),
        (
            'associated_programme_3_display',
            'associated_programme_3',
            'character varying(255)',
        ),
        (
            'associated_programme_4_display',
            'associated_programme_4',
            'character varying(255)',
        ),
        (
            'associated_programme_5_display',
            'associated_programme_5',
            'character varying(255)',
        ),
        ('audit', 'audit', 'text'),
        ('business_potential_display', 'business_potential', 'character varying(255)'),
        ('business_type', 'business_type', 'character varying(255)'),
        ('cdms_reference', 'cdms_reference', 'character varying(255)'),
        ('company_name', 'company_name', 'character varying(255)'),
        ('complete', 'complete', 'boolean'),
        (
            'confirmation__access_to_contacts',
            'confirmation_access_to_contacts',
            'integer',
        ),
        (
            'confirmation__access_to_information',
            'confirmation_access_to_information',
            'integer',
        ),
        ('confirmation__agree_with_win', 'confirmation_agree_with_win', 'boolean'),
        (
            'confirmation__case_study_willing',
            'confirmation_case_study_willing',
            'boolean',
        ),
        ('confirmation__comments', 'confirmation_comments', 'text'),
        (
            'confirmation__company_was_at_risk_of_not_exporting',
            'confirmation_company_was_at_risk_of_not_exporting',
            'boolean',
        ),
        ('confirmation__created', 'confirmation_created', 'timestamp with time zone'),
        (
            'confirmation__developed_relationships',
            'confirmation_developed_relationships',
            'integer',
        ),
        (
            'confirmation__gained_confidence',
            'confirmation_gained_confidence',
            'integer',
        ),
        (
            'confirmation__has_enabled_expansion_into_existing_market',
            'confirmation_has_enabled_expansion_into_existing_market',
            'boolean',
        ),
        (
            'confirmation__has_enabled_expansion_into_new_market',
            'confirmation_has_enabled_expansion_into_new_market',
            'boolean',
        ),
        (
            'confirmation__has_explicit_export_plans',
            'confirmation_has_explicit_export_plans',
            'boolean',
        ),
        ('confirmation__improved_profile', 'confirmation_improved_profile', 'integer'),
        (
            'confirmation__interventions_were_prerequisite',
            'confirmation_interventions_were_prerequisite',
            'boolean',
        ),
        (
            'confirmation__has_increased_exports_as_percent_of_turnover',
            'confirmation_has_increased_exports_as_percent_of_turnover',
            'boolean',
        ),
        (
            'confirmation__involved_state_enterprise',
            'confirmation_involved_state_enterprise',
            'boolean',
        ),
        ('confirmation__name', 'confirmation_name', 'character varying(255)'),
        (
            'confirmation__other_marketing_source',
            'confirmation_other_marketing_source',
            'character varying(255)',
        ),
        ('confirmation__our_support', 'confirmation_our_support', 'integer'),
        ('confirmation__overcame_problem', 'confirmation_overcame_problem', 'integer'),
        (
            'confirmation__support_improved_speed',
            'confirmation_support_improved_speed',
            'boolean',
        ),
        (
            'confirmation_last_export',
            'confirmation_last_export',
            'character varying(255)',
        ),
        (
            'confirmation_marketing_source',
            'confirmation_marketing_source',
            'character varying(255)',
        ),
        (
            'confirmation_portion_without_help',
            'confirmation_portion_without_help',
            'character varying(255)',
        ),
        ('country', 'country', 'character varying(255)'),
        ('created', 'created', 'timestamp with time zone'),
        ('customer_email_address', 'customer_email_address', 'character varying(255)'),
        ('customer_email_date', 'customer_email_date', 'timestamp with time zone'),
        ('customer_job_title', 'customer_job_title', 'character varying(255)'),
        ('customer_location_display', 'customer_location', 'character varying(255)'),
        ('customer_name', 'customer_name', 'character varying(255)'),
        ('date', 'date', 'date'),
        ('description', 'description', 'text'),
        ('export_experience_display', 'export_experience', 'character varying(255)'),
        ('goods_vs_services_display', 'goods_vs_services', 'character varying(255)'),
        ('has_hvo_specialist_involvement', 'has_hvo_specialist_involvement', 'boolean'),
        ('hq_team_display', 'hq_team', 'character varying(255)'),
        ('hvc', 'hvc', 'character varying(255)'),
        ('hvo_programme_display', 'hvo_programme', 'character varying(255)'),
        ('id', 'id', 'uuid primary key'),
        ('is_e_exported', 'is_e_exported', 'boolean'),
        ('is_line_manager_confirmed', 'is_line_manager_confirmed', 'boolean'),
        ('is_personally_confirmed', 'is_personally_confirmed', 'boolean'),
        ('is_prosperity_fund_related', 'is_prosperity_fund_related', 'boolean'),
        (
            'lead_officer_email_address',
            'lead_officer_email_address',
            'character varying(255)',
        ),
        ('lead_officer_name', 'lead_officer_name', 'character varying(255)'),
        ('line_manager_name', 'line_manager_name', 'character varying(255)'),
        ('name_of_customer', 'name_of_customer', 'character varying(255)'),
        ('name_of_export', 'name_of_export', 'character varying(255)'),
        ('num_notifications', 'num_notifications', 'integer'),
        (
            'other_official_email_address',
            'other_official_email_address',
            'character varying(255)',
        ),
        ('sector_display', 'sector', 'character varying(255)'),
        ('team_type_display', 'team_type', 'character varying(255)'),
        ('total_expected_export_value', 'total_expected_export_value', 'bigint'),
        (
            'total_expected_non_export_value',
            'total_expected_non_export_value',
            'bigint',
        ),
        ('total_expected_odi_value', 'total_expected_odi_value', 'bigint'),
        ('type_of_support_1_display', 'type_of_support_1', 'character varying(255)'),
        ('type_of_support_2_display', 'type_of_support_2', 'character varying(255)'),
        ('type_of_support_3_display', 'type_of_support_3', 'character varying(255)'),
        ('user__email', 'user_email', 'character varying(255)'),
        ('user__name', 'user_name', 'character varying(255)'),
    ]


for pipeline in BaseDatasetPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline.get_dag()
