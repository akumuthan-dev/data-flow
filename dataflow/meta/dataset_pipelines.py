"""A module that defines dataset pipeline meta objects."""

from datetime import datetime

from dataflow import constants


class OMISDatasetPipeline:
    """Pipeline meta object for OMISDataset."""

    table_name = 'omis_dataset'
    source_url = '{0}/v4/dataset/omis-dataset'.format(constants.DATAHUB_BASE_URL)
    target_db = 'datasets_db'
    start_date = datetime.now().replace(day=1)
    end_date = None
    schedule_interval = '@monthly'
    field_mapping = [
        (
            'reference',
            'omis_order_reference',
            'character varying(100)',
        ),
        (
            'company__name',
            'company_name',
            'character varying(255)',
        ),
        (
            'status',
            'order_status',
            'character varying(100)',
        ),
        (
            'contact__first_name',
            'contact_first_name',
            'character varying(255)',
        ),
        (
            'contact__last_name',
            'contact_last_name',
            'character varying(255)',
        ),
        (
            'contact__email',
            'contact_email',
            'character varying(255)',
        ),
        (
            'contact__telephone_number',
            'contact_phone_number',
            'character varying(255)',
        ),
        (
            'invoice__subtotal_cost',
            'subtotal',
            'integer',
        ),
        (
            'subtotal_cost',
            'net_price',
            'integer',
        ),
        (
            'sector__segment',
            'sector',
            'character varying(255)',
        ),
        (
            'primary_market__name',
            'market',
            'text',
        ),
        (
            'created_by__dit_team__name',
            'dit_team',
            'text',
        ),
        (
            'uk_region__name',
            'uk_region',
            'text',
        ),
        (
            'created_on',
            'created_date',
            'timestamp with time zone',
        ),
        (
            'cancelled_on',
            'cancelled_date',
            'timestamp with time zone',
        ),
        (
            'cancellation_reason__name',
            'cancellation_reason',
            'text',
        ),
        (
            'completed_on',
            'completion_date',
            'timestamp with time zone',
        ),
        (
            'delivery_date',
            'delivery_date',
            'date',
        ),
        (
            'paid_on',
            'payment_received_date',
            'timestamp with time zone',
        ),
        (
            'company__address_1',
            'company_trading_address_line_1',
            'character varying(255)',
        ),
        (
            'company__address_2',
            'company_trading_address_line_2',
            'character varying(255)',
        ),
        (
            'company__address_town',
            'company_trading_address_town',
            'character varying(255)',
        ),
        (
            'company__address_county',
            'company_trading_address_county',
            'character varying(255)',
        ),
        (
            'company__address_country__name',
            'company_trading_address_country',
            'character varying(255)',
        ),
        (
            'company__address_postcode',
            'company_trading_address_postcode',
            'character varying(255)',
        ),
        (
            'company__registered_address_1',
            'company_registered_address_1',
            'character varying(255)',
        ),
        (
            'company__registered_address_2',
            'company_registered_address_2',
            'character varying(255)',
        ),
        (
            'company__registered_address_town',
            'company_registered_address_town',
            'character varying(255)',
        ),
        (
            'company__registered_address_county',
            'company_registered_address_county',
            'character varying(255)',
        ),
        (
            'company__registered_address_country__name',
            'company_registered_address_country',
            'character varying(255)',
        ),
        (
            'company__registered_address_postcode',
            'company_registered_address_postcode',
            'character varying(255)',
        ),
        (
            'services',
            'services',
            'text',
        ),
    ]


class InvestmentProjectsDatasetPipeline:
    """Pipeline meta object for InvestmentProjectsDataset."""

    table_name = 'investment_projects_dataset'
    source_url = '{0}/v4/dataset/investment-projects-dataset'.format(constants.DATAHUB_BASE_URL)
    target_db = 'datasets_db'
    start_date = datetime.now().replace(day=1)
    end_date = None
    schedule_interval = '@daily'
    field_mapping = [
        (
            'account_manager_name',
            'account_manager_name',
            'text',
        ),
        (
            'account_manager_team',
            'account_manager_team',
            'text',
        ),
        (
            'actual_land_date',
            'actual_land_date',
            'date',
        ),
        (
            'actual_uk_region_names',
            'actual_uk_regions',
            'text',
        ),
        (
            'allow_blank_possible_uk_regions',
            'possible_uk_regions',
            'boolean',
        ),
        (
            'anonymous_description',
            'anonymous_description',
            'text',
        ),
        (
            'archived',
            'archived',
            'boolean',
        ),
        (
            'archived_by_name',
            'archived_by_name',
            'character varying(255)',
        ),
        (
            'archived_by__dit_team__name',
            'archived_by_team',
            'text',
        ),
        (
            'archived_on',
            'archived_on',
            'timestamp with time zone',
        ),
        (
            'associated_non_fdi_r_and_d_project__id',
            'associated_non_fdi_r_and_d_project_id',
            'character varying(100)',
        ),
        (
            'average_salary__name',
            'average_salary',
            'text',
        ),
        (
            'business_activity_names',
            'business_activities',
            'text',
        ),
        (
            'client_relationship_manager_name',
            'client_relationship_manager_name',
            'character varying(255)',
        ),
        (
            'client_relationship_manager__dit_team__name',
            'client_relationship_manager_team',
            'text',
        ),
        (
            'clint_requirements',
            'client_requirements',
            'text',
        ),
        (
            'competing_countries',
            'competing_countries',
            'text',
        ),
        (
            'country_lost_to__name',
            'country_lost_to',
            'text',
        ),
        (
            'created_by_name',
            'created_by_name',
            'character varying(255)',
        ),
        (
            'created_by__dit_team__name',
            'created_by_team',
            'text',
        ),
        (
            'created_on',
            'created_on',
            'timestamp with time zone',
        ),
        (
            'date_abandoned',
            'date_abandoned',
            'date',
        ),
        (
            'date_lost',
            'date_lost',
            'date',
        ),
        (
            'date_of_latest_interaction',
            'date_of_latest_interaction',
            'date',
        ),
        (
            'delivery_partner_names',
            'delivery_partners',
            'text',
        ),
        (
            'description',
            'description',
            'text',
        ),
        (
            'estimated_land_date',
            'estimated_land_date',
            'date',
        ),
        (
            'export_revenue',
            'export_revenue',
            'boolean',
        ),
        (
            'fdi_type__name',
            'fdi_type',
            'text',
        ),
        (
            'fdi_value__name',
            'fdi_value',
            'text',
        ),
        (
            'foreign_equity_investment',
            'foreign_equity_investment',
            'decimal',
        ),
        (
            'government_assistance',
            'government_assistance',
            'boolean',
        ),
        (
            'gross_value_added',
            'gross_value_added',
            'decimal',
        ),
        (
            'gva_multiplier__multiplier',
            'gva_multiplier',
            'decimal',
        ),
        (
            'id',
            'dh_fdi_project_id',
            'character varying(100)',
        ),
        (
            'investment_type__name',
            'investment_type',
            'text',
        ),
        (
            'investor_company__address_1',
            'investor_company_address_1',
            'character varying(255)',
        ),
        (
            'investor_company__address_2',
            'investor_company_address_2',
            'character varying(255)',
        ),
        (
            'investor_company__address_country__name',
            'investor_company_country',
            'character varying(255)',
        ),
        (
            'investor_company__address_postcode',
            'investor_company_address_postcode',
            'character varying(255)',
        ),
        (
            'investor_company__company_number',
            'investor_company_comp_house_id',
            'character varying(255)',
        ),
        (
            'investor_company__headquarter_type__name',
            'investor_company_headquarter_type',
            'text',
        ),
        (
            'investor_company__id',
            'investor_company_id',
            'character varying(100)',
        ),
        (
            'investor_company__name',
            'investor_company_name',
            'character varying(255)',
        ),
        (
            'investor_company__one_list_tier__name',
            'investor_company_company_tier',
            'text',
        ),
        (
            'investor_company_contact_accepts_dit_marketing',
            'investor_company_contact_accepts_dit_email_marketing',
            'boolean',
        ),
        (
            'investor_company_contact_email',
            'investor_company_contact_email',
            'character varying(255)',
        ),
        (
            'investor_company_contact_marked_as_primary_contact',
            'investor_company_contact_marked_as_primary_contact',
            'boolean',
        ),
        (
            'investor_company_contact_name',
            'investor_company_contact_name',
            'character varying(255)',
        ),
        (
            'investor_company_contact_phone',
            'investor_company_contact_phone',
            'character varying(255)',
        ),
        (
            'investor_company_sector',
            'investor_company_sector',
            'character varying(255)',
        ),
        (
            'investor_company__uk_region__name',
            'investor_company_uk_region',
            'text',
        ),
        (
            'investor_type__name',
            'investor_type',
            'text',
        ),
        (
            'level_of_involvement_name',
            'level_of_involvement',
            'text',
        ),
        (
            'likelihood_to_land__name',
            'likelihood_to_land',
            'text',
        ),
        (
            'modified_by_name',
            'modified_by_name',
            'character varying(255)',
        ),
        (
            'modified_by__dit_team__name',
            'modified_by_team',
            'text',
        ),
        (
            'modified_on',
            'modified_on',
            'timestamp with time zone',
        ),
        (
            'name',
            'name',
            'character varying(255) NOT NULL',
        ),
        (
            'new_tech_to_uk',
            'new_tech_to_uk',
            'boolean',
        ),
        (
            'non_fdi_r_and_d_budget',
            'non_fdi_r_and_d_budget',
            'boolean',
        ),
        (
            'number_new_jobs',
            'number_new_jobs',
            'integer',
        ),
        (
            'number_safeguarded_jobs',
            'number_safeguarded_jobs',
            'integer',
        ),
        (
            'project_arrived_in_triage_on',
            'project_arrived_in_triage_on',
            'date',
        ),
        (
            'project_assurance_adviser_name',
            'project_assurance_adviser_name',
            'character varying(255)',
        ),
        (
            'project_assurance_adviser__dit_team__name',
            'project_assurance_adviser_team',
            'text',
        ),
        (
            'project_manager_name',
            'project_manager_name',
            'character varying(255)',
        ),
        (
            'project_manager__dit_team__name',
            'project_manager_team',
            'text',
        ),
        (
            'project_reference',
            'project_reference',
            'text',
        ),
        (
            'proposal_deadline',
            'proposal_deadline',
            'date',
        ),
        (
            'quotable_as_public_case_study',
            'quotable_as_public_case_study',
            'boolean',
        ),
        (
            'r_and_d_budget',
            'r_and_d_budget',
            'boolean',
        ),
        (
            'referral_source_activity__name',
            'referral_source_activity',
            'text',
        ),
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
        (
            'sector_name',
            'sector',
            'character varying(255)',
        ),
        (
            'some_new_jobs',
            'some_new_jobs',
            'boolean',
        ),
        (
            'specific_programme__name',
            'specific_programme',
            'text',
        ),
        (
            'stage__name',
            'stage',
            'text NOT NULL',
        ),
        (
            'status',
            'status',
            'character varying(255)',
        ),
        (
            'strategic_driver_names',
            'strategic_drivers',
            'text',
        ),
        (
            'team_member_names',
            'team_members',
            'text',
        ),
        (
            'total_investment',
            'total_investment',
            'decimal',
        ),
        (
            'uk_company__address_1',
            'uk_company_address',
            'character varying(255)',
        ),
        (
            'uk_company__address_country__name',
            'uk_company_country',
            'character varying(255)',
        ),
        (
            'uk_company__address_postcode',
            'uk_company_postcode',
            'character varying(255)',
        ),
        (
            'uk_company__company_number',
            'uk_company_comp_house_id',
            'character varying(255)',
        ),
        (
            'uk_company__id',
            'uk_company_id',
            'character varying(100)',
        ),
        (
            'uk_company__name',
            'uk_company_name',
            'character varying(255)',
        ),
        (
            'uk_company__uk_region__name',
            'uk_company_uk_region',
            'text',
        ),
        (
            'uk_company_contact_accepts_dit_email_marketing',
            'uk_company_contact_accepts_dit_email_marketing',
            'boolean',
        ),
        (
            'uk_company_contact_email',
            'uk_company_contact_email',
            'character varying(255)',
        ),
        (
            'uk_company_contact_marked_as_primary_contact',
            'uk_company_contact_marked_as_primary_contact',
            'boolean',
        ),
        (
            'uk_company_contact_name',
            'uk_company_contact_name',
            'character varying(255)',
        ),
        (
            'uk_company_contact_phone',
            'uk_company_contact_phone',
            'character varying(255)',
        ),
        (
            'uk_company_decided',
            'uk_company_decided',
            'boolean',
        ),
        (
            'uk_company_sector',
            'uk_company_sector',
            'character varying(255)',
        ),
    ]
