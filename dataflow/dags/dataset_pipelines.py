"""A module that defines Airflow DAGS for dataset pipelines."""
from datetime import datetime, timedelta

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow import config
from dataflow.operators.dataset import fetch_from_api
from dataflow.operators.db_tables import (
    check_table_data,
    create_temp_tables,
    insert_data_into_db,
    swap_dataset_table,
    drop_temp_tables,
)


class BaseDatasetPipeline:
    target_db = config.DATASETS_DB_NAME
    start_date = datetime(2019, 11, 5)
    end_date = None
    schedule_interval = '@daily'

    @property
    def table(self):
        if not hasattr(self, "_table"):
            meta = sa.MetaData()
            self._table = sa.Table(
                self.table_name,
                meta,
                *[column.copy() for _, column in self.field_mapping],
            )
        return self._table

    def get_dag(self):
        with DAG(
            self.__class__.__name__,
            catchup=False,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
            max_active_runs=1,
        ) as dag:

            _fetch = PythonOperator(
                task_id='run-fetch',
                python_callable=fetch_from_api,
                provide_context=True,
                op_args=[self.table_name, self.source_url],
            )

            _create_tables = PythonOperator(
                task_id='create-temp-tables',
                python_callable=create_temp_tables,
                provide_context=True,
                op_args=[self.target_db, self.table],
            )

            _insert_into_temp_table = PythonOperator(
                task_id="insert-into-temp-table",
                python_callable=insert_data_into_db,
                provide_context=True,
                op_args=[self.target_db, self.table, self.field_mapping],
            )

            _check_tables = PythonOperator(
                task_id="check-temp-table-data",
                python_callable=check_table_data,
                provide_context=True,
                op_args=[self.target_db, self.table],
            )

            _swap_dataset_table = PythonOperator(
                task_id="swap-dataset-table",
                python_callable=swap_dataset_table,
                provide_context=True,
                op_args=[self.target_db, self.table],
            )

            _drop_tables = PythonOperator(
                task_id="drop-temp-tables",
                python_callable=drop_temp_tables,
                provide_context=True,
                trigger_rule="all_done",
                op_args=[self.target_db, self.table],
                op_kwargs={'cascade': True},
            )

        (
            [_fetch, _create_tables]
            >> _insert_into_temp_table
            >> _check_tables
            >> _swap_dataset_table
            >> _drop_tables
        )

        return dag


class CompanyExportToCountries(BaseDatasetPipeline):

    table_name = 'company_export_to_countries_dataset'
    source_url = '{0}/v4/dataset/company-export-to-countries-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('id', sa.Column('id', sa.Integer, primary_key=True)),
        ('company_id', sa.Column('company_id', UUID)),
        ('country__name', sa.Column('country', sa.String)),
        ('country__iso_alpha2_code', sa.Column('country_iso_alpha2_code', sa.String)),
    ]


class CompanyFutureInterestCountries(BaseDatasetPipeline):

    table_name = 'company_future_interest_countries_dataset'
    source_url = '{0}/v4/dataset/company-future-interest-countries-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('id', sa.Column('id', sa.Integer, primary_key=True)),
        ('company_id', sa.Column('company_id', UUID)),
        ('country__name', sa.Column('country', sa.String)),
        ('country__iso_alpha2_code', sa.Column('country_iso_alpha2_code', sa.String)),
    ]


class CountriesOfInterestServicePipeline(BaseDatasetPipeline):
    table_name = 'countries_of_interest_dataset'
    source_url = (
        '{0}/api/v1/get-company-countries-and-sectors-of-interest'
        '?orientation=records'.format(config.COUNTRIES_OF_INTEREST_BASE_URL)
    )
    field_mapping = [
        ('serviceCompanyId', sa.Column('service_company_id', sa.Text)),
        ('companyMatchId', sa.Column('company_match_id', sa.Text)),
        ('country', sa.Column('country', sa.Text)),
        ('sector', sa.Column('sector', sa.Text)),
        ('service', sa.Column('service', sa.Text)),
        ('type', sa.Column('type', sa.Text)),
        ('source', sa.Column('source', sa.Text)),
        ('sourceId', sa.Column('source_id', sa.Text)),
        ('timestamp', sa.Column('timestamp', sa.DateTime)),
    ]


class OMISDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for OMISDataset."""

    table_name = 'omis_dataset'
    source_url = '{0}/v4/dataset/omis-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('cancellation_reason__name', sa.Column('cancellation_reason', sa.Text)),
        ('cancelled_on', sa.Column('cancelled_date', sa.DateTime)),
        ('company_id', sa.Column('company_id', UUID)),
        ('completed_on', sa.Column('completion_date', sa.DateTime)),
        ('contact_id', sa.Column('contact_id', UUID)),
        ('created_by__dit_team_id', sa.Column('dit_team_id', UUID)),
        ('created_on', sa.Column('created_date', sa.DateTime)),
        ('delivery_date', sa.Column('delivery_date', sa.Date)),
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('invoice__subtotal_cost', sa.Column('subtotal', sa.Integer)),
        ('paid_on', sa.Column('payment_received_date', sa.DateTime)),
        ('primary_market__name', sa.Column('market', sa.Text)),
        ('quote__accepted_on', sa.Column('quote_accepted_on', sa.DateTime)),
        ('quote__created_on', sa.Column('quote_created_on', sa.DateTime)),
        ('reference', sa.Column('omis_order_reference', sa.String)),
        ('refund_created', sa.Column('refund_created', sa.DateTime)),
        ('refund_total_amount', sa.Column('refund_total_amount', sa.Integer)),
        ('sector_name', sa.Column('sector', sa.String)),
        ('services', sa.Column('services', sa.Text)),
        ('status', sa.Column('order_status', sa.String)),
        ('subtotal_cost', sa.Column('net_price', sa.Numeric(asdecimal=True))),
        ('total_cost', sa.Column('total_cost', sa.Integer)),
        ('uk_region__name', sa.Column('uk_region', sa.Text)),
        ('vat_cost', sa.Column('vat_cost', sa.Integer)),
    ]


class InvestmentProjectsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for InvestmentProjectsDataset."""

    table_name = 'investment_projects_dataset'
    source_url = '{0}/v4/dataset/investment-projects-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    field_mapping = [
        ('actual_land_date', sa.Column('actual_land_date', sa.Date)),
        ('actual_uk_region_names', sa.Column('actual_uk_regions', sa.Text)),
        ('address_1', sa.Column('address_1', sa.String)),
        ('address_2', sa.Column('address_2', sa.String)),
        ('address_town', sa.Column('address_town', sa.String)),
        ('address_postcode', sa.Column('address_postcode', sa.String)),
        ('anonymous_description', sa.Column('anonymous_description', sa.Text)),
        (
            'associated_non_fdi_r_and_d_project__id',
            sa.Column('associated_non_fdi_r_and_d_project_id', sa.String),
        ),
        ('average_salary__name', sa.Column('average_salary', sa.Text)),
        ('business_activity_names', sa.Column('business_activities', sa.Text)),
        (
            'client_relationship_manager_id',
            sa.Column('client_relationship_manager_id', UUID),
        ),
        ('client_requirements', sa.Column('client_requirements', sa.Text)),
        ('competing_countries', sa.Column('competing_countries', sa.ARRAY(sa.Text))),
        ('created_by_id', sa.Column('created_by_id', UUID)),
        ('created_on', sa.Column('created_on', sa.DateTime)),
        ('delivery_partner_names', sa.Column('delivery_partners', sa.Text)),
        ('description', sa.Column('description', sa.Text)),
        ('estimated_land_date', sa.Column('estimated_land_date', sa.Date)),
        ('export_revenue', sa.Column('export_revenue', sa.Boolean)),
        ('fdi_type__name', sa.Column('fdi_type', sa.Text)),
        ('fdi_value__name', sa.Column('fdi_value', sa.Text)),
        (
            'foreign_equity_investment',
            sa.Column('foreign_equity_investment', sa.Numeric(asdecimal=True)),
        ),
        ('government_assistance', sa.Column('government_assistance', sa.Boolean)),
        (
            'gross_value_added',
            sa.Column('gross_value_added', sa.Numeric(asdecimal=True)),
        ),
        (
            'gva_multiplier__multiplier',
            sa.Column('gva_multiplier', sa.Numeric(asdecimal=True)),
        ),
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('investment_type__name', sa.Column('investment_type', sa.Text)),
        ('investor_company_id', sa.Column('investor_company_id', UUID)),
        ('investor_company_sector', sa.Column('investor_company_sector', sa.String)),
        ('investor_type__name', sa.Column('investor_type', sa.Text)),
        ('level_of_involvement_name', sa.Column('level_of_involvement', sa.Text)),
        ('likelihood_to_land__name', sa.Column('likelihood_to_land', sa.Text)),
        ('modified_by_id', sa.Column('modified_by_id', UUID)),
        ('modified_on', sa.Column('modified_on', sa.DateTime)),
        ('name', sa.Column('name', sa.String, nullable=False)),
        ('new_tech_to_uk', sa.Column('new_tech_to_uk', sa.Boolean)),
        ('non_fdi_r_and_d_budget', sa.Column('non_fdi_r_and_d_budget', sa.Boolean)),
        ('number_new_jobs', sa.Column('number_new_jobs', sa.Integer)),
        ('number_safeguarded_jobs', sa.Column('number_safeguarded_jobs', sa.Integer)),
        ('other_business_activity', sa.Column('other_business_activity', sa.String)),
        (
            'project_arrived_in_triage_on',
            sa.Column('project_arrived_in_triage_on', sa.Date),
        ),
        (
            'project_assurance_adviser_id',
            sa.Column('project_assurance_adviser_id', UUID),
        ),
        ('project_manager_id', sa.Column('project_manager_id', UUID)),
        ('project_reference', sa.Column('project_reference', sa.Text)),
        ('proposal_deadline', sa.Column('proposal_deadline', sa.Date)),
        ('r_and_d_budget', sa.Column('r_and_d_budget', sa.Boolean)),
        (
            'referral_source_activity__name',
            sa.Column('referral_source_activity', sa.Text),
        ),
        (
            'referral_source_activity_marketing__name',
            sa.Column('referral_source_activity_marketing', sa.Text),
        ),
        (
            'referral_source_activity_website__name',
            sa.Column('referral_source_activity_website', sa.Text),
        ),
        ('sector_name', sa.Column('sector', sa.String)),
        ('specific_programme__name', sa.Column('specific_programme', sa.Text)),
        ('stage__name', sa.Column('stage', sa.Text, nullable=False)),
        ('status', sa.Column('status', sa.String)),
        ('strategic_driver_names', sa.Column('strategic_drivers', sa.Text)),
        ('team_member_ids', sa.Column('team_member_ids', sa.ARRAY(sa.Text))),
        ('total_investment', sa.Column('total_investment', sa.Numeric(asdecimal=True))),
        ('uk_company_id', sa.Column('uk_company_id', UUID)),
        ('uk_company_sector', sa.Column('uk_company_sector', sa.String)),
        ('uk_region_location_names', sa.Column('possible_uk_regions', sa.Text)),
    ]


class InteractionsDatasetPipeline(BaseDatasetPipeline):
    table_name = 'interactions_dataset'
    source_url = '{}/v4/dataset/interactions-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('adviser_ids', sa.Column('adviser_ids', sa.ARRAY(sa.Text))),
        ('communication_channel__name', sa.Column('communication_channel', sa.String)),
        ('company_id', sa.Column('company_id', UUID)),
        ('contact_ids', sa.Column('contact_ids', sa.ARRAY(sa.Text))),
        ('created_on', sa.Column('created_on', sa.DateTime)),
        ('date', sa.Column('interaction_date', sa.Date)),
        ('event_id', sa.Column('event_id', UUID)),
        (
            'grant_amount_offered',
            sa.Column('grant_amount_offered', sa.Numeric(asdecimal=True)),
        ),
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('interaction_link', sa.Column('interaction_link', sa.String)),
        ('investment_project_id', sa.Column('investment_project_id', UUID)),
        ('kind', sa.Column('interaction_kind', sa.String)),
        (
            'net_company_receipt',
            sa.Column('net_company_receipt', sa.Numeric(asdecimal=True)),
        ),
        ('notes', sa.Column('interaction_notes', sa.Text)),
        ('sector', sa.Column('sector', sa.String)),
        (
            'service_delivery_status__name',
            sa.Column('service_delivery_status', sa.String),
        ),
        ('service_delivery', sa.Column('service_delivery', sa.String)),
        ('subject', sa.Column('interaction_subject', sa.Text)),
        ('theme', sa.Column('theme', sa.String)),
        ('policy_feedback_notes', sa.Column('policy_feedback_notes', sa.Text)),
        ('policy_area_names', sa.Column('policy_areas', sa.ARRAY(sa.Text))),
        ('policy_issue_type_names', sa.Column('policy_issue_types', sa.ARRAY(sa.Text))),
    ]


class ContactsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for ContactsDataset."""

    table_name = 'contacts_dataset'
    source_url = '{0}/v4/dataset/contacts-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        (
            'accepts_dit_email_marketing',
            sa.Column('accepts_dit_email_marketing', sa.Boolean),
        ),
        ('address_1', sa.Column('address_1', sa.String)),
        ('address_2', sa.Column('address_2', sa.String)),
        ('address_country__name', sa.Column('address_country', sa.Text)),
        ('address_county', sa.Column('address_county', sa.String)),
        ('address_postcode', sa.Column('address_postcode', sa.String)),
        ('address_same_as_company', sa.Column('address_same_as_company', sa.Boolean)),
        ('address_town', sa.Column('address_town', sa.String)),
        ('archived', sa.Column('archived', sa.Boolean)),
        ('archived_on', sa.Column('archived_on', sa.DateTime)),
        ('company_id', sa.Column('company_id', UUID)),
        ('created_on', sa.Column('date_added_to_datahub', sa.Date)),
        ('email', sa.Column('email', sa.String)),
        ('email_alternative', sa.Column('email_alternative', sa.String)),
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('job_title', sa.Column('job_title', sa.String)),
        ('modified_on', sa.Column('modified_on', sa.DateTime)),
        ('name', sa.Column('contact_name', sa.Text)),
        ('notes', sa.Column('notes', sa.Text)),
        ('primary', sa.Column('is_primary', sa.Boolean)),
        ('telephone_alternative', sa.Column('telephone_alternative', sa.String)),
        ('telephone_number', sa.Column('phone', sa.String)),
    ]


class CompaniesDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for CompaniesDataset."""

    table_name = 'companies_dataset'
    source_url = '{0}/v4/dataset/companies-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('address_1', sa.Column('address_1', sa.String)),
        ('address_2', sa.Column('address_2', sa.String)),
        ('address_county', sa.Column('address_county', sa.String)),
        ('address_country__name', sa.Column('address_country', sa.String)),
        ('address_postcode', sa.Column('address_postcode', sa.String)),
        ('address_town', sa.Column('address_town', sa.String)),
        ('archived', sa.Column('archived', sa.Boolean)),
        ('archived_on', sa.Column('archived_on', sa.DateTime)),
        ('business_type__name', sa.Column('business_type', sa.String)),
        ('company_number', sa.Column('company_number', sa.String)),
        ('created_on', sa.Column('created_on', sa.Date)),
        ('description', sa.Column('description', sa.Text)),
        ('duns_number', sa.Column('duns_number', sa.String)),
        ('export_experience_category__name', sa.Column('export_experience', sa.String)),
        ('headquarter_type__name', sa.Column('headquarter_type', sa.String)),
        ('id', sa.Column('id', UUID, primary_key=True)),
        (
            'is_number_of_employees_estimated',
            sa.Column('is_number_of_employees_estimated', sa.Boolean),
        ),
        ('is_turnover_estimated', sa.Column('is_turnover_estimated', sa.Boolean)),
        ('modified_on', sa.Column('modified_on', sa.DateTime)),
        ('name', sa.Column('name', sa.String)),
        ('number_of_employees', sa.Column('number_of_employees', sa.Integer)),
        ('one_list_account_owner_id', sa.Column('one_list_account_owner_id', UUID)),
        ('one_list_tier__name', sa.Column('classification', sa.String)),
        ('reference_code', sa.Column('cdms_reference_code', sa.String)),
        ('registered_address_1', sa.Column('registered_address_1', sa.String)),
        ('registered_address_2', sa.Column('registered_address_2', sa.String)),
        (
            'registered_address_country__name',
            sa.Column('registered_address_country', sa.String),
        ),
        (
            'registered_address_county',
            sa.Column('registered_address_county', sa.String),
        ),
        (
            'registered_address_postcode',
            sa.Column('registered_address_postcode', sa.String),
        ),
        ('registered_address_town', sa.Column('registered_address_town', sa.String)),
        ('sector_name', sa.Column('sector', sa.String)),
        ('trading_names', sa.Column('trading_names', sa.String)),
        ('turnover', sa.Column('turnover', sa.BigInteger)),
        ('uk_region__name', sa.Column('uk_region', sa.String)),
        ('vat_number', sa.Column('vat_number', sa.String)),
        ('website', sa.Column('website', sa.String)),
    ]


class AdvisersDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for AdvisersDataset."""

    table_name = 'advisers_dataset'
    source_url = '{0}/v4/dataset/advisers-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('date_joined', sa.Column('date_joined', sa.Date)),
        ('first_name', sa.Column('first_name', sa.String)),
        ('last_name', sa.Column('last_name', sa.String)),
        ('telephone_number', sa.Column('telephone_number', sa.String)),
        ('contact_email', sa.Column('contact_email', sa.String)),
        ('dit_team_id', sa.Column('team_id', UUID)),
        ('is_active', sa.Column('is_active', sa.Boolean)),
    ]


class TeamsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for TeamsDataset."""

    table_name = 'teams_dataset'
    source_url = '{0}/v4/dataset/teams-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('name', sa.Column('name', sa.String)),
        ('role__name', sa.Column('role', sa.String)),
        ('uk_region__name', sa.Column('uk_region', sa.String)),
        ('country__name', sa.Column('country', sa.String)),
    ]


class EventsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for EventsDataset."""

    table_name = 'events_dataset'
    source_url = '{0}/v4/dataset/events-dataset'.format(config.DATAHUB_BASE_URL)
    field_mapping = [
        ('address_1', sa.Column('address_1', sa.String)),
        ('address_2', sa.Column('address_2', sa.String)),
        ('address_country__name', sa.Column('address_country', sa.String)),
        ('address_county', sa.Column('address_county', sa.String)),
        ('address_postcode', sa.Column('address_postcode', sa.String)),
        ('address_town', sa.Column('address_town', sa.String)),
        ('created_on', sa.Column('created_on', sa.DateTime)),
        ('end_date', sa.Column('end_date', sa.Date)),
        ('event_type__name', sa.Column('event_type', sa.String)),
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('lead_team_id', sa.Column('lead_team_id', UUID)),
        ('location_type__name', sa.Column('location_type', sa.String)),
        ('name', sa.Column('name', sa.String)),
        ('notes', sa.Column('notes', sa.Text)),
        ('organiser_id', sa.Column('organiser_id', UUID)),
        ('service_name', sa.Column('service_name', sa.String)),
        ('start_date', sa.Column('start_date', sa.Date)),
        ('team_ids', sa.Column('team_ids', sa.ARRAY(sa.Text))),
        ('uk_region__name', sa.Column('uk_region', sa.String)),
    ]


class ExportWinsAdvisersDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins advisers."""

    table_name = 'export_wins_advisers_dataset'
    source_url = '{0}/datasets/advisors'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        ('hq_team_display', sa.Column('hq_team', sa.String)),
        ('id', sa.Column('id', sa.Integer, primary_key=True)),
        ('location', sa.Column('location', sa.String)),
        ('name', sa.Column('name', sa.String)),
        ('team_type_display', sa.Column('team_type', sa.String)),
        ('win__id', sa.Column('win_id', UUID)),
    ]


class ExportWinsBreakdownsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins yearly export/non-export value."""

    table_name = 'export_wins_breakdowns_dataset'
    source_url = '{0}/datasets/breakdowns'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        ('breakdown_type', sa.Column('type', sa.String)),
        ('id', sa.Column('id', sa.Integer, primary_key=True)),
        ('value', sa.Column('value', sa.BigInteger)),
        ('win__id', sa.Column('win_id', UUID)),
        ('year', sa.Column('year', sa.Integer)),
    ]


class ExportWinsHVCDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins HVC data."""

    table_name = 'export_wins_hvc_dataset'
    source_url = '{0}/datasets/hvc'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        ('campaign_id', sa.Column('campaign_id', sa.String)),
        ('financial_year', sa.Column('financial_year', sa.Integer)),
        ('id', sa.Column('id', sa.Integer, primary_key=True)),
        ('name', sa.Column('name', sa.String)),
    ]


class ExportWinsWinsDatasetPipeline(BaseDatasetPipeline):
    """Pipeline meta object for Export wins wins."""

    table_name = 'export_wins_wins_dataset'
    source_url = '{0}/datasets/wins'.format(config.EXPORT_WINS_BASE_URL)
    field_mapping = [
        (
            'associated_programme_1_display',
            sa.Column('associated_programme_1', sa.String),
        ),
        (
            'associated_programme_2_display',
            sa.Column('associated_programme_2', sa.String),
        ),
        (
            'associated_programme_3_display',
            sa.Column('associated_programme_3', sa.String),
        ),
        (
            'associated_programme_4_display',
            sa.Column('associated_programme_4', sa.String),
        ),
        (
            'associated_programme_5_display',
            sa.Column('associated_programme_5', sa.String),
        ),
        ('audit', sa.Column('audit', sa.Text)),
        ('business_potential_display', sa.Column('business_potential', sa.String)),
        ('business_type', sa.Column('business_type', sa.String)),
        ('cdms_reference', sa.Column('cdms_reference', sa.String)),
        ('company_name', sa.Column('company_name', sa.String)),
        ('complete', sa.Column('complete', sa.Boolean)),
        (
            'confirmation__access_to_contacts',
            sa.Column('confirmation_access_to_contacts', sa.Integer),
        ),
        (
            'confirmation__access_to_information',
            sa.Column('confirmation_access_to_information', sa.Integer),
        ),
        (
            'confirmation__agree_with_win',
            sa.Column('confirmation_agree_with_win', sa.Boolean),
        ),
        (
            'confirmation__case_study_willing',
            sa.Column('confirmation_case_study_willing', sa.Boolean),
        ),
        ('confirmation__comments', sa.Column('confirmation_comments', sa.Text)),
        (
            'confirmation__company_was_at_risk_of_not_exporting',
            sa.Column('confirmation_company_was_at_risk_of_not_exporting', sa.Boolean),
        ),
        ('confirmation__created', sa.Column('confirmation_created', sa.DateTime)),
        (
            'confirmation__developed_relationships',
            sa.Column('confirmation_developed_relationships', sa.Integer),
        ),
        (
            'confirmation__gained_confidence',
            sa.Column('confirmation_gained_confidence', sa.Integer),
        ),
        (
            'confirmation__has_enabled_expansion_into_existing_market',
            sa.Column(
                'confirmation_has_enabled_expansion_into_existing_market', sa.Boolean
            ),
        ),
        (
            'confirmation__has_enabled_expansion_into_new_market',
            sa.Column('confirmation_has_enabled_expansion_into_new_market', sa.Boolean),
        ),
        (
            'confirmation__has_explicit_export_plans',
            sa.Column('confirmation_has_explicit_export_plans', sa.Boolean),
        ),
        (
            'confirmation__improved_profile',
            sa.Column('confirmation_improved_profile', sa.Integer),
        ),
        (
            'confirmation__interventions_were_prerequisite',
            sa.Column('confirmation_interventions_were_prerequisite', sa.Boolean),
        ),
        (
            'confirmation__has_increased_exports_as_percent_of_turnover',
            sa.Column(
                'confirmation_has_increased_exports_as_percent_of_turnover', sa.Boolean
            ),
        ),
        (
            'confirmation__involved_state_enterprise',
            sa.Column('confirmation_involved_state_enterprise', sa.Boolean),
        ),
        ('confirmation__name', sa.Column('confirmation_name', sa.String)),
        (
            'confirmation__other_marketing_source',
            sa.Column('confirmation_other_marketing_source', sa.String),
        ),
        (
            'confirmation__our_support',
            sa.Column('confirmation_our_support', sa.Integer),
        ),
        (
            'confirmation__overcame_problem',
            sa.Column('confirmation_overcame_problem', sa.Integer),
        ),
        (
            'confirmation__support_improved_speed',
            sa.Column('confirmation_support_improved_speed', sa.Boolean),
        ),
        ('confirmation_last_export', sa.Column('confirmation_last_export', sa.String)),
        (
            'confirmation_marketing_source',
            sa.Column('confirmation_marketing_source', sa.String),
        ),
        (
            'confirmation_portion_without_help',
            sa.Column('confirmation_portion_without_help', sa.String),
        ),
        ('country', sa.Column('country_code', sa.String)),
        ('country_name', sa.Column('country', sa.String)),
        ('created', sa.Column('created', sa.DateTime)),
        ('customer_email_address', sa.Column('customer_email_address', sa.String)),
        ('customer_email_date', sa.Column('customer_email_date', sa.DateTime)),
        ('customer_job_title', sa.Column('customer_job_title', sa.String)),
        ('customer_location_display', sa.Column('customer_location', sa.String)),
        ('customer_name', sa.Column('customer_name', sa.String)),
        ('date', sa.Column('date', sa.Date)),
        ('description', sa.Column('description', sa.Text)),
        ('export_experience_display', sa.Column('export_experience', sa.String)),
        ('goods_vs_services_display', sa.Column('goods_vs_services', sa.String)),
        (
            'has_hvo_specialist_involvement',
            sa.Column('has_hvo_specialist_involvement', sa.Boolean),
        ),
        ('hq_team_display', sa.Column('hq_team', sa.String)),
        ('hvc', sa.Column('hvc', sa.String)),
        ('hvo_programme_display', sa.Column('hvo_programme', sa.String)),
        ('id', sa.Column('id', UUID, primary_key=True)),
        ('is_e_exported', sa.Column('is_e_exported', sa.Boolean)),
        (
            'is_line_manager_confirmed',
            sa.Column('is_line_manager_confirmed', sa.Boolean),
        ),
        ('is_personally_confirmed', sa.Column('is_personally_confirmed', sa.Boolean)),
        (
            'is_prosperity_fund_related',
            sa.Column('is_prosperity_fund_related', sa.Boolean),
        ),
        (
            'lead_officer_email_address',
            sa.Column('lead_officer_email_address', sa.String),
        ),
        ('lead_officer_name', sa.Column('lead_officer_name', sa.String)),
        ('line_manager_name', sa.Column('line_manager_name', sa.String)),
        ('name_of_customer', sa.Column('name_of_customer', sa.String)),
        ('name_of_export', sa.Column('name_of_export', sa.String)),
        ('num_notifications', sa.Column('num_notifications', sa.Integer)),
        (
            'other_official_email_address',
            sa.Column('other_official_email_address', sa.String),
        ),
        ('sector_display', sa.Column('sector', sa.String)),
        ('team_type_display', sa.Column('team_type', sa.String)),
        (
            'total_expected_export_value',
            sa.Column('total_expected_export_value', sa.BigInteger),
        ),
        (
            'total_expected_non_export_value',
            sa.Column('total_expected_non_export_value', sa.BigInteger),
        ),
        (
            'total_expected_odi_value',
            sa.Column('total_expected_odi_value', sa.BigInteger),
        ),
        ('type_of_support_1_display', sa.Column('type_of_support_1', sa.String)),
        ('type_of_support_2_display', sa.Column('type_of_support_2', sa.String)),
        ('type_of_support_3_display', sa.Column('type_of_support_3', sa.String)),
        ('user__email', sa.Column('user_email', sa.String)),
        ('user__name', sa.Column('user_name', sa.String)),
    ]


for pipeline in BaseDatasetPipeline.__subclasses__():
    globals()[pipeline.__name__ + "__dag"] = pipeline().get_dag()
