"""A module that defines Airflow DAGS for dataset pipelines."""
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow import config
from dataflow.config import DATAHUB_HAWK_CREDENTIALS
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hawk_api
from dataflow.utils import TableConfig


class _DatasetPipeline(_PipelineDAG):
    cascade_drop_tables = True

    source_url: str

    fetch_retries = 0

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_hawk_api, hawk_credentials=DATAHUB_HAWK_CREDENTIALS
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
            retries=self.fetch_retries,
        )


class CompanyExportCountryHistory(_DatasetPipeline):

    source_url = '{0}/v4/dataset/company-export-country-history-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    table_config = TableConfig(
        table_name='company_export_country_history_dataset',
        field_mapping=[
            ('company_id', sa.Column('company_id', UUID)),
            ('country__name', sa.Column("country", sa.String)),
            (
                'country__iso_alpha2_code',
                sa.Column('country_iso_alpha2_code', sa.String),
            ),
            ('history_id', sa.Column('id', UUID, primary_key=True)),
            ('history_date', sa.Column('history_date', sa.DateTime)),
            ('history_type', sa.Column('history_type', sa.String)),
            ('status', sa.Column('status', sa.String)),
        ],
    )


class CompanyExportCountry(_DatasetPipeline):

    source_url = '{0}/v4/dataset/company-export-country-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    table_config = TableConfig(
        table_name='company_export_country_dataset',
        field_mapping=[
            ('company_id', sa.Column('company_id', UUID)),
            ('country__name', sa.Column("country", sa.String)),
            (
                'country__iso_alpha2_code',
                sa.Column('country_iso_alpha2_code', sa.String),
            ),
            ('created_on', sa.Column('created_on', sa.DateTime)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('modified_on', sa.Column('modified_on', sa.DateTime)),
            ('status', sa.Column('status', sa.String)),
        ],
    )


class CountriesOfInterestServicePipeline(_DatasetPipeline):
    source_url = (
        '{0}/api/v1/get-company-countries-and-sectors-of-interest'
        '?orientation=records'.format(config.COUNTRIES_OF_INTEREST_BASE_URL)
    )
    table_config = TableConfig(
        table_name='countries_of_interest_dataset',
        field_mapping=[
            ('serviceCompanyId', sa.Column('service_company_id', sa.Text)),
            ('companyMatchId', sa.Column('company_match_id', sa.Text)),
            ('country', sa.Column('country', sa.Text)),
            ('sector', sa.Column('sector', sa.Text)),
            ('service', sa.Column('service', sa.Text)),
            ('type', sa.Column('type', sa.Text)),
            ('source', sa.Column('source', sa.Text)),
            ('sourceId', sa.Column('source_id', sa.Text)),
            ('timestamp', sa.Column('timestamp', sa.DateTime)),
        ],
    )


class OMISDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for OMISDataset."""

    source_url = '{0}/v4/dataset/omis-dataset'.format(config.DATAHUB_BASE_URL)
    table_config = TableConfig(
        table_name='omis_dataset',
        field_mapping=[
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
            ('subtotal_cost', sa.Column('net_price', sa.Numeric)),
            ('total_cost', sa.Column('total_cost', sa.Integer)),
            ('uk_region__name', sa.Column('uk_region', sa.Text)),
            ('vat_cost', sa.Column('vat_cost', sa.Integer)),
        ],
    )


class InvestmentProjectsDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for InvestmentProjectsDataset."""

    source_url = '{0}/v4/dataset/investment-projects-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    allow_null_columns = True
    table_config = TableConfig(
        table_name='investment_projects_dataset',
        field_mapping=[
            ('actual_land_date', sa.Column('actual_land_date', sa.Date)),
            (
                'actual_uk_region_names',
                sa.Column('actual_uk_regions', sa.ARRAY(sa.Text)),
            ),
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
            (
                'business_activity_names',
                sa.Column('business_activities', sa.ARRAY(sa.Text)),
            ),
            (
                'client_relationship_manager_id',
                sa.Column('client_relationship_manager_id', UUID),
            ),
            ('client_requirements', sa.Column('client_requirements', sa.Text)),
            (
                'competing_countries',
                sa.Column('competing_countries', sa.ARRAY(sa.Text)),
            ),
            ('created_by_id', sa.Column('created_by_id', UUID)),
            ('created_on', sa.Column('created_on', sa.DateTime)),
            (
                'delivery_partner_names',
                sa.Column('delivery_partners', sa.ARRAY(sa.Text)),
            ),
            ('description', sa.Column('description', sa.Text)),
            ('estimated_land_date', sa.Column('estimated_land_date', sa.Date)),
            ('export_revenue', sa.Column('export_revenue', sa.Boolean)),
            ('fdi_type__name', sa.Column('fdi_type', sa.Text)),
            ('fdi_value__name', sa.Column('fdi_value', sa.Text)),
            (
                'foreign_equity_investment',
                sa.Column('foreign_equity_investment', sa.Numeric),
            ),
            ('government_assistance', sa.Column('government_assistance', sa.Boolean)),
            ('gross_value_added', sa.Column('gross_value_added', sa.Numeric)),
            ('gva_multiplier__multiplier', sa.Column('gva_multiplier', sa.Numeric)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('investment_type__name', sa.Column('investment_type', sa.Text)),
            ('investor_company_id', sa.Column('investor_company_id', UUID)),
            (
                'investor_company_sector',
                sa.Column('investor_company_sector', sa.String),
            ),
            ('investor_type__name', sa.Column('investor_type', sa.Text)),
            ('level_of_involvement_name', sa.Column('level_of_involvement', sa.Text)),
            ('likelihood_to_land__name', sa.Column('likelihood_to_land', sa.Text)),
            ('modified_by_id', sa.Column('modified_by_id', UUID)),
            ('modified_on', sa.Column('modified_on', sa.DateTime)),
            ('name', sa.Column('name', sa.String, nullable=False)),
            ('new_tech_to_uk', sa.Column('new_tech_to_uk', sa.Boolean)),
            ('non_fdi_r_and_d_budget', sa.Column('non_fdi_r_and_d_budget', sa.Boolean)),
            ('number_new_jobs', sa.Column('number_new_jobs', sa.Integer)),
            (
                'number_safeguarded_jobs',
                sa.Column('number_safeguarded_jobs', sa.Integer),
            ),
            (
                'other_business_activity',
                sa.Column('other_business_activity', sa.String),
            ),
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
            (
                'strategic_driver_names',
                sa.Column('strategic_drivers', sa.ARRAY(sa.Text)),
            ),
            ('team_member_ids', sa.Column('team_member_ids', sa.ARRAY(sa.Text))),
            ('total_investment', sa.Column('total_investment', sa.Numeric)),
            ('uk_company_id', sa.Column('uk_company_id', UUID)),
            ('uk_company_sector', sa.Column('uk_company_sector', sa.String)),
            (
                'uk_region_location_names',
                sa.Column('possible_uk_regions', sa.ARRAY(sa.Text)),
            ),
        ],
    )


class InteractionsDatasetPipeline(_DatasetPipeline):
    source_url = '{}/v4/dataset/interactions-dataset?page_size=10000'.format(
        config.DATAHUB_BASE_URL
    )
    table_config = TableConfig(
        table_name='interactions_dataset',
        field_mapping=[
            ('adviser_ids', sa.Column('adviser_ids', sa.ARRAY(sa.Text))),
            (
                'communication_channel__name',
                sa.Column('communication_channel', sa.String),
            ),
            ('company_id', sa.Column('company_id', UUID)),
            ('contact_ids', sa.Column('contact_ids', sa.ARRAY(sa.Text))),
            ('created_by_id', sa.Column('created_by_id', UUID)),
            ('created_on', sa.Column('created_on', sa.DateTime)),
            ('date', sa.Column('interaction_date', sa.Date)),
            ('event_id', sa.Column('event_id', UUID)),
            ('grant_amount_offered', sa.Column('grant_amount_offered', sa.Numeric)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('interaction_link', sa.Column('interaction_link', sa.String)),
            ('investment_project_id', sa.Column('investment_project_id', UUID)),
            ('kind', sa.Column('interaction_kind', sa.String)),
            ('modified_on', sa.Column('modified_on', sa.DateTime)),
            ('net_company_receipt', sa.Column('net_company_receipt', sa.Numeric)),
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
            (
                'policy_issue_type_names',
                sa.Column('policy_issue_types', sa.ARRAY(sa.Text)),
            ),
            (
                'were_countries_discussed',
                sa.Column('were_countries_discussed', sa.Boolean),
            ),
        ],
    )
    fetch_retries = 3


class InteractionsExportCountryDatasetPipeline(_DatasetPipeline):
    source_url = '{}/v4/dataset/interactions-export-country-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    table_config = TableConfig(
        table_name='interactions_export_country',
        field_mapping=[
            ('country__name', sa.Column('country_name', sa.Text)),
            ('country__iso_alpha2_code', sa.Column('country_iso_alpha2_code', sa.Text)),
            ('created_on', sa.Column('created_on', sa.DateTime)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('interaction__company_id', sa.Column('company_id', UUID)),
            ('interaction__id', sa.Column('interaction_id', UUID)),
            ('status', sa.Column('status', sa.Text)),
        ],
    )


class ContactsDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for ContactsDataset."""

    source_url = '{0}/v4/dataset/contacts-dataset'.format(config.DATAHUB_BASE_URL)
    table_config = TableConfig(
        table_name='contacts_dataset',
        field_mapping=[
            (
                'accepts_dit_email_marketing',
                sa.Column('accepts_dit_email_marketing', sa.Boolean),
            ),
            ('address_1', sa.Column('address_1', sa.String)),
            ('address_2', sa.Column('address_2', sa.String)),
            ('address_country__name', sa.Column('address_country', sa.Text)),
            ('address_county', sa.Column('address_county', sa.String)),
            ('address_postcode', sa.Column('address_postcode', sa.String)),
            (
                'address_same_as_company',
                sa.Column('address_same_as_company', sa.Boolean),
            ),
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
        ],
    )


class CompaniesDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for CompaniesDataset."""

    source_url = '{0}/v4/dataset/companies-dataset'.format(config.DATAHUB_BASE_URL)
    table_config = TableConfig(
        table_name='companies_dataset',
        field_mapping=[
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
            (
                'export_experience_category__name',
                sa.Column('export_experience', sa.String),
            ),
            ('global_headquarters_id', sa.Column('global_headquarters_id', UUID)),
            (
                'global_ultimate_duns_number',
                sa.Column('global_ultimate_duns_number', sa.String),
            ),
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
            ('one_list_tier__name', sa.Column('one_list_tier', sa.String)),
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
            (
                'registered_address_town',
                sa.Column('registered_address_town', sa.String),
            ),
            ('sector_name', sa.Column('sector', sa.String)),
            ('trading_names', sa.Column('trading_names', sa.String)),
            ('turnover', sa.Column('turnover', sa.BigInteger)),
            ('uk_region__name', sa.Column('uk_region', sa.String)),
            ('vat_number', sa.Column('vat_number', sa.String)),
            ('website', sa.Column('website', sa.String)),
        ],
    )


class AdvisersDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for AdvisersDataset."""

    source_url = '{0}/v4/dataset/advisers-dataset'.format(config.DATAHUB_BASE_URL)
    table_config = TableConfig(
        table_name='advisers_dataset',
        field_mapping=[
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('date_joined', sa.Column('date_joined', sa.Date)),
            ('first_name', sa.Column('first_name', sa.String)),
            ('last_name', sa.Column('last_name', sa.String)),
            ('telephone_number', sa.Column('telephone_number', sa.String)),
            ('contact_email', sa.Column('contact_email', sa.String)),
            ('dit_team_id', sa.Column('team_id', UUID)),
            ('is_active', sa.Column('is_active', sa.Boolean)),
        ],
    )


class TeamsDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for TeamsDataset."""

    source_url = '{0}/v4/dataset/teams-dataset'.format(config.DATAHUB_BASE_URL)
    table_config = TableConfig(
        table_name='teams_dataset',
        field_mapping=[
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('name', sa.Column('name', sa.String)),
            ('role__name', sa.Column('role', sa.String)),
            ('uk_region__name', sa.Column('uk_region', sa.String)),
            ('country__name', sa.Column('country', sa.String)),
        ],
    )


class EventsDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for EventsDataset."""

    source_url = '{0}/v4/dataset/events-dataset'.format(config.DATAHUB_BASE_URL)
    table_config = TableConfig(
        table_name='events_dataset',
        field_mapping=[
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
        ],
    )


class ExportWinsAdvisersDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for Export wins advisers."""

    source_url = '{0}/datasets/advisors'.format(config.EXPORT_WINS_BASE_URL)
    table_config = TableConfig(
        table_name='export_wins_advisers_dataset',
        field_mapping=[
            ('hq_team_display', sa.Column('hq_team', sa.String)),
            ('id', sa.Column('id', sa.Integer, primary_key=True)),
            ('location', sa.Column('location', sa.String)),
            ('name', sa.Column('name', sa.String)),
            ('team_type_display', sa.Column('team_type', sa.String)),
            ('win__id', sa.Column('win_id', UUID)),
        ],
    )


class ExportWinsBreakdownsDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for Export wins yearly export/non-export value."""

    source_url = '{0}/datasets/breakdowns'.format(config.EXPORT_WINS_BASE_URL)
    table_config = TableConfig(
        table_name='export_wins_breakdowns_dataset',
        field_mapping=[
            ('breakdown_type', sa.Column('type', sa.String)),
            ('id', sa.Column('id', sa.Integer, primary_key=True)),
            ('value', sa.Column('value', sa.BigInteger)),
            ('win__id', sa.Column('win_id', UUID)),
            ('year', sa.Column('year', sa.Integer)),
        ],
    )


class ExportWinsHVCDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for Export wins HVC data."""

    source_url = '{0}/datasets/hvc'.format(config.EXPORT_WINS_BASE_URL)
    table_config = TableConfig(
        table_name='export_wins_hvc_dataset',
        field_mapping=[
            ('campaign_id', sa.Column('campaign_id', sa.String)),
            ('financial_year', sa.Column('financial_year', sa.Integer)),
            ('id', sa.Column('id', sa.Integer, primary_key=True)),
            ('name', sa.Column('name', sa.String)),
        ],
    )


class ExportWinsWinsDatasetPipeline(_DatasetPipeline):
    """Pipeline meta object for Export wins wins."""

    source_url = '{0}/datasets/wins'.format(config.EXPORT_WINS_BASE_URL)
    allow_null_columns = True
    table_config = TableConfig(
        table_name='export_wins_wins_dataset',
        field_mapping=[
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
                sa.Column(
                    'confirmation_company_was_at_risk_of_not_exporting', sa.Boolean
                ),
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
                    'confirmation_has_enabled_expansion_into_existing_market',
                    sa.Boolean,
                ),
            ),
            (
                'confirmation__has_enabled_expansion_into_new_market',
                sa.Column(
                    'confirmation_has_enabled_expansion_into_new_market', sa.Boolean
                ),
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
                    'confirmation_has_increased_exports_as_percent_of_turnover',
                    sa.Boolean,
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
            (
                'confirmation_last_export',
                sa.Column('confirmation_last_export', sa.String),
            ),
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
            (
                'is_personally_confirmed',
                sa.Column('is_personally_confirmed', sa.Boolean),
            ),
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
        ],
    )


class ONSPostcodePipeline(_DatasetPipeline):
    """Pipeline meta object for ONS Postcode data."""

    source_url = '{0}/api/v1/get-ons-postcodes/?orientation=records'.format(
        config.DATA_STORE_SERVICE_BASE_URL
    )
    table_config = TableConfig(
        table_name='ons_postcodes',
        field_mapping=[
            ('pcd', sa.Column('pcd', sa.Text)),
            ('pcd2', sa.Column('pcd2', sa.Text)),
            ('pcds', sa.Column('pcds', sa.Text)),
            ('dointr', sa.Column('dointr', sa.Date)),
            ('doterm', sa.Column('doterm', sa.Date)),
            ('oscty', sa.Column('oscty', sa.Text)),
            ('ced', sa.Column('ced', sa.Text)),
            ('oslaua', sa.Column('oslaua', sa.Text)),
            ('osward', sa.Column('osward', sa.Text)),
            ('parish', sa.Column('parish', sa.Text)),
            ('usertype', sa.Column('usertype', sa.Text)),
            ('oseast1m', sa.Column('oseast1m', sa.Text)),
            ('osnrth1m', sa.Column('osnrth1m', sa.Text)),
            ('osgrdind', sa.Column('osgrdind', sa.Text)),
            ('oshlthau', sa.Column('oshlthau', sa.Text)),
            ('nhser', sa.Column('nhser', sa.Text)),
            ('ctry', sa.Column('ctry', sa.Text)),
            ('rgn', sa.Column('rgn', sa.Text)),
            ('streg', sa.Column('streg', sa.Text)),
            ('pcon', sa.Column('pcon', sa.Text)),
            ('eer', sa.Column('eer', sa.Text)),
            ('teclec', sa.Column('teclec', sa.Text)),
            ('ttwa', sa.Column('ttwa', sa.Text)),
            ('pct', sa.Column('pct', sa.Text)),
            ('nuts', sa.Column('nuts', sa.Text)),
            ('statsward', sa.Column('statsward', sa.Text)),
            ('oa01', sa.Column('oa01', sa.Text)),
            ('casward', sa.Column('casward', sa.Text)),
            ('park', sa.Column('park', sa.Text)),
            ('lsoa01', sa.Column('lsoa01', sa.Text)),
            ('msoa01', sa.Column('msoa01', sa.Text)),
            ('ur01ind', sa.Column('ur01ind', sa.Text)),
            ('oac01', sa.Column('oac01', sa.Text)),
            ('oa11', sa.Column('oa11', sa.Text)),
            ('lsoa11', sa.Column('lsoa11', sa.Text)),
            ('msoa11', sa.Column('msoa11', sa.Text)),
            ('wz11', sa.Column('wz11', sa.Text)),
            ('ccg', sa.Column('ccg', sa.Text)),
            ('bua11', sa.Column('bua11', sa.Text)),
            ('buasd11', sa.Column('buasd11', sa.Text)),
            ('ru11ind', sa.Column('ru11ind', sa.Text)),
            ('oac11', sa.Column('oac11', sa.Text)),
            ('lat', sa.Column('lat', sa.Text)),
            ('long', sa.Column('long', sa.Text)),
            ('lep1', sa.Column('lep1', sa.Text)),
            ('lep2', sa.Column('lep2', sa.Text)),
            ('pfa', sa.Column('pfa', sa.Text)),
            ('imd', sa.Column('imd', sa.Text)),
            ('calncv', sa.Column('calncv', sa.Text)),
            ('stp', sa.Column('stp', sa.Text)),
        ],
    )


class RawWorldBankTariffPipeline(_DatasetPipeline):
    """Pipeline meta object for the raw world bank tariff data."""

    schedule_interval = '@yearly'
    source_url = (
        f'{config.DATA_STORE_SERVICE_BASE_URL}/api/v1/get-world-bank-tariffs/raw/'
        f'?orientation=records'
    )
    table_config = TableConfig(
        table_name='raw_world_bank_tariffs',
        field_mapping=[
            ('product', sa.Column('product', sa.Integer, index=True)),
            ('partner', sa.Column('partner', sa.Integer, index=True)),
            ('reporter', sa.Column('reporter', sa.Integer, index=True)),
            ('year', sa.Column('year', sa.Integer, index=True)),
            ('simpleAverage', sa.Column('simple_average', sa.Numeric)),
            ('dutyType', sa.Column('duty_type', sa.Text)),
            ('numberOfTotalLines', sa.Column('number_of_total_lines', sa.Numeric)),
        ],
    )


class RawWorldBankBoundRatePipeline(_DatasetPipeline):
    """Pipeline meta object for the raw world bank bound rates data."""

    schedule_interval = '@yearly'
    source_url = (
        f'{config.DATA_STORE_SERVICE_BASE_URL}/api/v1/get-world-bank-bound-rates/raw/'
        f'?orientation=records'
    )
    table_config = TableConfig(
        table_name='raw_world_bank_bound_rates',
        field_mapping=[
            ('product', sa.Column('product', sa.Integer, index=True)),
            ('reporter', sa.Column('reporter', sa.Integer, index=True)),
            ('boundRate', sa.Column('bound_rate', sa.Numeric)),
            ('nomenCode', sa.Column('nomen_code', sa.Text)),
            ('totalNumberOfLines', sa.Column('number_of_total_lines', sa.Numeric)),
        ],
    )
