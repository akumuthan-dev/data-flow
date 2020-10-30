"""Airflow Pipeline DAGs containing aggregate data for publishing on dashboards"""

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from airflow.operators.python_operator import PythonOperator

from dataflow.dags.base import _PipelineDAG
from dataflow.dags.dataset_pipelines import (
    AdvisersDatasetPipeline,
    CompaniesDatasetPipeline,
    ContactsDatasetPipeline,
    InteractionsDatasetPipeline,
    InvestmentProjectsDatasetPipeline,
    ONSPostcodePipeline,
    TeamsDatasetPipeline,
)
from dataflow.operators.db_tables import query_database
from dataflow.utils import TableConfig


class _SQLPipelineDAG(_PipelineDAG):
    schedule_interval = '@daily'
    query: str
    use_utc_now_as_source_modified = True

    def get_fetch_operator(self):
        return PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[
                self.query,
                self.target_db,
                self.table_config.table_name,  # pylint: disable=no-member
            ],
        )


class FDIDashboardPipeline(_SQLPipelineDAG):
    dependencies = [CompaniesDatasetPipeline, InvestmentProjectsDatasetPipeline]
    start_date = datetime(2020, 3, 3)
    table_config = TableConfig(
        table_name="fdi_dashboard_data",
        field_mapping=[
            ('fdi_value', sa.Column('fdi_value', sa.Text)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            (
                'investor_company_country',
                sa.Column('investor_company_country', sa.String),
            ),
            ('number_new_jobs', sa.Column('number_new_jobs', sa.Integer)),
            (
                'number_safeguarded_jobs',
                sa.Column('number_safeguarded_jobs', sa.Integer),
            ),
            ('overseas_region', sa.Column('overseas_region', sa.String)),
            ('project_end_date', sa.Column('project_end_date', sa.Date)),
            ('project_link', sa.Column('project_link', sa.String)),
            ('project_reference', sa.Column('project_reference', sa.String)),
            ('project_sector', sa.Column('project_sector', sa.String)),
            ('sector_cluster', sa.Column('sector_cluster', sa.String)),
            ('stage', sa.Column('stage', sa.String)),
            ('status', sa.Column('status', sa.String)),
            ('total_investment', sa.Column('total_investment', sa.Numeric)),
        ],
    )
    query = '''
        SELECT
            investment_projects_dataset.id,
            investment_projects_dataset.project_reference,
            CONCAT('https://datahub.trade.gov.uk/investments/projects/', investment_projects_dataset.id, '/details') AS project_link,
            investment_projects_dataset.total_investment,
            investment_projects_dataset.number_new_jobs,
            investment_projects_dataset.number_safeguarded_jobs,
            investment_projects_dataset.stage,
            investment_projects_dataset.status::TEXT,
            investment_projects_dataset.fdi_value,
            data_hub__companies.address_country::TEXT AS investor_company_country,
            ref_countries_territories_and_regions.region::TEXT as overseas_region,
            SPLIT_PART(investment_projects_dataset.sector, ' : ', 1) AS project_sector,
            ref_dit_sectors.field_03::TEXT AS sector_cluster,
            CASE
                WHEN investment_projects_dataset.stage IN ('Prospect', 'Assign PM', 'Active')
                THEN investment_projects_dataset.estimated_land_date
                ELSE investment_projects_dataset.actual_land_date
            END AS project_end_date
        FROM investment_projects_dataset
        JOIN dit.data_hub__companies ON data_hub__companies.id = investment_projects_dataset.investor_company_id
        JOIN ref_dit_sectors ON ref_dit_sectors.full_sector_name = investment_projects_dataset.sector
        LEFT JOIN ref_countries_territories_and_regions ON ref_countries_territories_and_regions.name = data_hub__companies.address_country
        WHERE (
            investment_projects_dataset.estimated_land_date BETWEEN '2020-04-01' AND '2021-03-31'
            OR
            investment_projects_dataset.actual_land_date BETWEEN '2020-04-01' AND '2021-03-31'
        )
        AND investment_projects_dataset.investment_type = 'FDI'
        AND investment_projects_dataset.level_of_involvement != 'No Involvement'
        AND (investment_projects_dataset.status = 'ongoing' OR investment_projects_dataset.status = 'won')
    '''


class CoronavirusInteractionsDashboardPipeline(_SQLPipelineDAG):
    start_date = datetime(2020, 3, 25)
    dependencies = [
        InteractionsDatasetPipeline,
        AdvisersDatasetPipeline,
        TeamsDatasetPipeline,
        ContactsDatasetPipeline,
    ]

    table_config = TableConfig(
        table_name="coronavirus_interactions_dashboard_data",
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ("interaction_date", sa.Column("interaction_date", sa.Date)),
            ("company_name", sa.Column("company_name", sa.Text)),
            ("company_country", sa.Column("company_country", sa.Text)),
            ("company_link", sa.Column("company_link", sa.Text)),
            ("company_sector", sa.Column("company_sector", sa.Text)),
            ("company_region", sa.Column("company_region", sa.Text)),
            ("subject_of_interaction", sa.Column("subject_of_interaction", sa.Text)),
            ("data_hub_link", sa.Column("data_hub_link", sa.Text)),
            ("team", sa.Column("team", sa.Text)),
            ("role", sa.Column("role", sa.Text)),
            ("policy_areas", sa.Column("policy_areas", sa.ARRAY(sa.Text))),
            ("entered_into_data_hub", sa.Column("entered_into_data_hub", sa.DateTime)),
        ],
    )

    query = '''
    with covid_interactions as (select * from dit.data_hub__interactions
    where
    (
        (interaction_subject ILIKE '%coronavirus%' or interaction_subject ILIKE '%covid%')
        or
        (interaction_notes ILIKE '%coronavirus%' or interaction_notes ILIKE '%covid%')
        or
        (policy_feedback_notes ILIKE '%coronavirus%' or policy_feedback_notes ILIKE '%covid%')
        or (
            policy_areas::text LIKE '%Coronavirus%'
        )
    )
    and interaction_date > '2019-12-05'),
    c_advisers as (
        select data_hub__advisers.id as "id", teams_dataset.name as team, teams_dataset.role as role from data_hub__advisers
        join teams_dataset on teams_dataset.id = data_hub__advisers.team_id
        where data_hub__advisers.id in (select unnest(covid_interactions.adviser_ids)::uuid from covid_interactions)
    ),
    c_contacts as (
        select data_hub__contacts.id, contact_name from dit.data_hub__contacts
        where data_hub__contacts.id in (select unnest(covid_interactions.contact_ids)::uuid from covid_interactions)
    )
    select
    ci.interaction_date::text as "interaction_date",
    co.name as "company_name",
    co.address_country as "company_country",
    concat('https://www.datahub.trade.gov.uk/companies/', co.id, '/activity') as "company_link",
    co.sector as "company_sector",
    co.uk_region as "company_region",
    ci.interaction_subject as "subject_of_interaction",
    ci.interaction_link as "data_hub_link",
    (select c_advisers.team from c_advisers where c_advisers.id = ci.adviser_ids[1]::uuid) as "team",
    (select c_advisers.role from c_advisers where c_advisers.id = ci.adviser_ids[1]::uuid) as "role",
    ci.policy_areas as "policy_areas",
    ci.created_on::text as "entered_into_data_hub"
    from covid_interactions ci
    join dit.data_hub__companies co on co.id = ci.company_id
    order by ci.interaction_date DESC;
    '''


class MinisterialInteractionsDashboardPipeline(_SQLPipelineDAG):
    """
    A dashboard of Data Hub interactions involving Ministers and certain senior staff
    """

    start_date = datetime(2020, 5, 6)
    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        InteractionsDatasetPipeline,
        ONSPostcodePipeline,
    ]
    table_config = TableConfig(
        table_name='ministerial_interactions',
        field_mapping=[
            (None, sa.Column('id', sa.Integer, primary_key=True, autoincrement=True)),
            ('interaction_id', sa.Column('interaction_id', UUID)),
            ('interaction_link', sa.Column('interaction_link', sa.Text)),
            ('company_name', sa.Column('company_name', sa.Text)),
            ('company_link', sa.Column('company_link', sa.Text)),
            ('company_country', sa.Column('company_country', sa.Text)),
            ('company_postcode', sa.Column('company_postcode', sa.Text)),
            (
                'company_address_latitude_longitude',
                sa.Column('company_address_latitude_longitude', sa.Text),
            ),
            ('company_uk_region', sa.Column('company_uk_region', sa.Text)),
            (
                'company_number_of_employees',
                sa.Column('company_number_of_employees', sa.Text),
            ),
            ('company_turnover', sa.Column('company_turnover', sa.Text)),
            ('company_one_list_tier', sa.Column('company_one_list_tier', sa.Text)),
            ('company_sector', sa.Column('company_sector', sa.Text)),
            ('interaction_date', sa.Column('interaction_date', sa.Date)),
            ('adviser_id', sa.Column('adviser_id', UUID)),
            ('adviser_name', sa.Column('adviser_name', sa.Text)),
            ('interaction_subject', sa.Column('interaction_subject', sa.Text)),
            ('communication_channel', sa.Column('communication_channel', sa.Text)),
            ('policy_areas', sa.Column('policy_areas', sa.Text)),
            ('policy_feedback_notes', sa.Column('policy_feedback_notes', sa.Text)),
            ('policy_issue_types', sa.Column('policy_issue_types', sa.Text)),
            (
                'dun_and_bradstreet_linked_record',
                sa.Column('dun_and_bradstreet_linked_record', sa.Text),
            ),
            ('service', sa.Column('service', sa.Text)),
        ],
    )
    query = '''
        WITH minister_interactions AS (
        SELECT
            unnest(data_hub__interactions.adviser_ids) AS adviser_id,
            data_hub__interactions.*
        FROM dit.data_hub__interactions
        WHERE
            '19d855be-a8bb-4004-8ae7-d52902e3f85a' = ANY(adviser_ids)
            OR 'c3680c0e-fd4b-4b1b-8def-46c23f8cd97c' = ANY(adviser_ids)
            OR '16a1bfae-ad92-4c4d-b29d-15755bc862a4' = ANY(adviser_ids)
            OR '16ac8d25-3bd8-4a1e-825c-02f5b409ea4f' = ANY(adviser_ids)
            OR '6658a92a-52e3-469f-b637-fb8768670d0f' = ANY(adviser_ids)
            OR '5b8e813e-9e6b-47e5-86ea-c4965cd45958' = ANY(adviser_ids)
            OR '763f4b9f-0f61-4273-85cd-d5c16b17c8a0' = ANY(adviser_ids)
            OR '3dbade20-03f2-45f1-b073-09f096c19990' = ANY(adviser_ids)
        )
        SELECT
            minister_interactions.id AS interaction_id,
            minister_interactions.interaction_link AS interaction_link,
            data_hub__companies.name AS company_name,
            CONCAT('https://datahub.trade.gov.uk/companies/',data_hub__companies.id,'/activity') AS company_link,
            data_hub__companies.address_country AS company_country,
            data_hub__companies.address_postcode AS company_postcode,
            CASE
                WHEN postcode_directory__latest.long IS NOT NULL
                THEN CONCAT(postcode_directory__latest.lat, ', ', postcode_directory__latest.long)
                END AS company_address_latitude_longitude,
            data_hub__companies.uk_region AS company_uk_region,
            data_hub__companies.number_of_employees AS company_number_of_employees,
            data_hub__companies.turnover AS company_turnover,
            data_hub__companies.one_list_tier AS company_one_list_tier,
            SPLIT_PART(data_hub__companies.sector, ' : ', 1) AS company_sector,
            minister_interactions.interaction_date AS interaction_date,
            minister_interactions.adviser_id::UUID AS adviser_id,
            CONCAT(data_hub__advisers.first_name,' ',data_hub__advisers.last_name) AS adviser_name,
            minister_interactions.interaction_subject AS interaction_subject,
            minister_interactions.communication_channel AS communication_channel,
            array_to_string(minister_interactions.policy_areas, ', ') AS policy_areas,
            minister_interactions.policy_feedback_notes AS policy_feedback_notes,
            array_to_string(minister_interactions.policy_issue_types, ', ') AS policy_issue_types,
            CASE
                WHEN data_hub__companies.duns_number IS NULL
                THEN 'No'
                ELSE 'Yes'
            END AS dun_and_bradstreet_linked_record,
            minister_interactions.service_delivery AS service
        FROM minister_interactions
        JOIN dit.data_hub__companies ON data_hub__companies.id = minister_interactions.company_id
        JOIN dit.data_hub__advisers ON data_hub__advisers.id = minister_interactions.adviser_id::uuid
        LEFT JOIN ons.postcode_directory__latest ON REPLACE(postcode_directory__latest.pcd2,' ','') = REPLACE(data_hub__companies.address_postcode, ' ','')
        WHERE adviser_id IN (
            '19d855be-a8bb-4004-8ae7-d52902e3f85a',
            'c3680c0e-fd4b-4b1b-8def-46c23f8cd97c',
            '16a1bfae-ad92-4c4d-b29d-15755bc862a4',
            '16ac8d25-3bd8-4a1e-825c-02f5b409ea4f',
            '6658a92a-52e3-469f-b637-fb8768670d0f',
            '5b8e813e-9e6b-47e5-86ea-c4965cd45958',
            '763f4b9f-0f61-4273-85cd-d5c16b17c8a0',
            '3dbade20-03f2-45f1-b073-09f096c19990'
        )
        ORDER BY data_hub__companies.name ASC
    '''


class DataHubMonthlyInvesmentProjectsPipline(_SQLPipelineDAG):
    """
    For creation of dashboards to be used by the Investment Promotion Performance team
    """

    start_date = datetime(2020, 8, 1)
    schedule_interval = '@monthly'
    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        TeamsDatasetPipeline,
        InteractionsDatasetPipeline,
        InvestmentProjectsDatasetPipeline,
    ]
    table_config = TableConfig(
        schema='dit',
        table_name='data_hub__investment_projects__monthly',
        field_mapping=[
            ('investment_project_id', sa.Column('investment_project_id', UUID)),
            ('project_reference_code', sa.Column('project_reference_code', sa.Text)),
            ('project_name', sa.Column('project_name', sa.Text)),
            ('description', sa.Column('description', sa.Text)),
            ('actual_land_date', sa.Column('actual_land_date', sa.Date)),
            ('estimated_land_date', sa.Column('estimated_land_date', sa.Date)),
            ('created_on', sa.Column('created_on', sa.Date)),
            ('modified_on', sa.Column('modified_on', sa.Date)),
            (
                'date_of_latest_interaction',
                sa.Column('date_of_latest_interaction', sa.Date),
            ),
            ('fdi_value', sa.Column('fdi_value', sa.Text)),
            ('investment_type', sa.Column('investment_type', sa.Text)),
            ('fdi_type', sa.Column('fdi_type', sa.Text)),
            ('business_activities', sa.Column('business_activities', sa.Text)),
            ('investor_type', sa.Column('investor_type', sa.Text)),
            ('specific_programme', sa.Column('specific_programme', sa.Text)),
            ('level_of_involvement', sa.Column('level_of_involvement', sa.Text)),
            ('project_stage', sa.Column('project_stage', sa.Text)),
            ('project_status', sa.Column('project_status', sa.Text)),
            ('competing_countries', sa.Column('competing_countries', sa.Text)),
            (
                'referral_source_activity',
                sa.Column('referral_source_activity', sa.Text),
            ),
            (
                'referral_source_activity_website',
                sa.Column('referral_source_activity_website', sa.Text),
            ),
            (
                'referral_source_activity_marketing',
                sa.Column('referral_source_activity_marketing', sa.Text),
            ),
            ('total_investment', sa.Column('total_investment', sa.Numeric)),
            (
                'foreign_equity_investment',
                sa.Column('foreign_equity_investment', sa.Numeric),
            ),
            ('gross_value_added', sa.Column('gross_value_added', sa.Numeric)),
            ('investment_value_band', sa.Column('investment_value_band', sa.Text)),
            ('government_assistance', sa.Column('government_assistance', sa.Boolean)),
            ('r_and_d_budget', sa.Column('r_and_d_budget', sa.Boolean)),
            ('non_fdi_r_and_d_budget', sa.Column('non_fdi_r_and_d_budget', sa.Boolean)),
            ('new_tech_to_uk', sa.Column('new_tech_to_uk', sa.Boolean)),
            ('number_new_jobs', sa.Column('number_new_jobs', sa.Integer)),
            (
                'number_safeguarded_jobs',
                sa.Column('number_safeguarded_jobs', sa.Integer),
            ),
            ('total_jobs', sa.Column('total_jobs', sa.Integer)),
            ('average_salary', sa.Column('average_salary', sa.Text)),
            ('client_requirements', sa.Column('client_requirements', sa.Text)),
            ('anonymous_description', sa.Column('anonymous_description', sa.Text)),
            (
                'investor_company_country',
                sa.Column('investor_company_country', sa.Text),
            ),
            ('hmtc', sa.Column('hmtc', sa.Text)),
            ('original_project_sector', sa.Column('original_project_sector', sa.Text)),
            ('project_sector', sa.Column('project_sector', sa.Text)),
            ('dit_sector_cluster', sa.Column('dit_sector_cluster', sa.Text)),
            ('possible_uk_regions', sa.Column('possible_uk_regions', sa.Text)),
            ('actual_uk_regions', sa.Column('actual_uk_regions', sa.Text)),
            ('delivery_partners', sa.Column('delivery_partners', sa.Text)),
            ('super_region', sa.Column('super_region', sa.Text)),
            ('investor_company_name', sa.Column('investor_company_name', sa.Text)),
            (
                'investor_company_comp_house_id',
                sa.Column('investor_company_comp_house_id', sa.Text),
            ),
            ('headquarter_type', sa.Column('headquarter_type', sa.Text)),
            (
                'investor_company_company_tier',
                sa.Column('investor_company_company_tier', sa.Text),
            ),
            ('address_1', sa.Column('address_1', sa.Text)),
            ('address_2', sa.Column('address_2', sa.Text)),
            ('address_postcode', sa.Column('address_postcode', sa.Text)),
            ('uk_company_name', sa.Column('uk_company_name', sa.Text)),
            (
                'uk_company_comp_house_id',
                sa.Column('uk_company_comp_house_id', sa.Text),
            ),
            ('uk_company_sector', sa.Column('uk_company_sector', sa.Text)),
            ('uk_company_address_1', sa.Column('uk_company_address_1', sa.Text)),
            ('uk_company_address_2', sa.Column('uk_company_address_2', sa.Text)),
            ('uk_company_uk_region', sa.Column('uk_company_uk_region', sa.Text)),
            ('created_by_name', sa.Column('created_by_name', sa.Text)),
            ('created_by_team', sa.Column('created_by_team', sa.Text)),
            ('modified_by_name', sa.Column('modified_by_name', sa.Text)),
            ('modified_by_team', sa.Column('modified_by_team', sa.Text)),
            (
                'client_relationship_manager_name',
                sa.Column('client_relationship_manager_name', sa.Text),
            ),
            (
                'client_relationship_manager_team',
                sa.Column('client_relationship_manager_team', sa.Text),
            ),
            ('project_manager_name', sa.Column('project_manager_name', sa.Text)),
            ('project_manager_team', sa.Column('project_manager_team', sa.Text)),
            (
                'project_assurance_adviser_name',
                sa.Column('project_assurance_adviser_name', sa.Text),
            ),
            (
                'project_assurance_adviser_team',
                sa.Column('project_assurance_adviser_team', sa.Text),
            ),
            ('account_manager_name', sa.Column('account_manager_name', sa.Text)),
            ('account_manager_team', sa.Column('account_manager_team', sa.Text)),
            ('team_members', sa.Column('team_members', sa.Text)),
            (
                'all_virtual_team_members',
                sa.Column('all_virtual_team_members', sa.Text),
            ),
            ('dit_hq_team_members', sa.Column('dit_hq_team_members', sa.Text)),
            ('ist_team_members', sa.Column('ist_team_members', sa.Text)),
            ('post_team', sa.Column('post_team', sa.Text)),
            ('delivery_partner_teams', sa.Column('delivery_partner_teams', sa.Text)),
            ('lep_or_da_in_team', sa.Column('lep_or_da_in_team', sa.Text)),
            (
                'super_region_or_da_in_team',
                sa.Column('super_region_or_da_in_team', sa.Text),
            ),
            ('export_revenue', sa.Column('export_revenue', sa.Boolean)),
        ],
    )
    query = '''
        WITH
        --Table converting all 'Retail' related projects to the Consumer and retail sector
        investment_projects AS (
            SELECT investment_projects_dataset.*,
                CASE
                    WHEN 'Retail' = ANY (investment_projects_dataset.business_activities::TEXT[])
                        THEN 'Consumer and retail'
                    ELSE investment_projects_dataset.sector
                END AS project_sector
            FROM investment_projects_dataset
            WHERE (
                (actual_land_date >= '2018-04-01' OR estimated_land_date >= '2018-04-01')
                AND LOWER(status) IN ('ongoing', 'won')
            )
            OR (
                modified_on BETWEEN (now() - interval '1 year') AND now()
                AND LOWER(status) NOT IN ('ongoing', 'won')
            )
        ),

        --Table of latest interaction date for each investment project
        latest_interactions AS (
            SELECT data_hub__interactions.investment_project_id,
                MAX(data_hub__interactions.interaction_date) AS date_of_latest_interaction
            FROM dit.data_hub__interactions
            WHERE data_hub__interactions.investment_project_id IS NOT NULL
            GROUP BY data_hub__interactions.investment_project_id),

        --Extract team member ids, names and teams
        project_team AS (
            SELECT
               investment_projects.id AS project_id,
                --ID, name and team name for various roles associated with the project
                created_by_adviser.id AS created_by_adviser_id,
                created_by_adviser.first_name || ' ' || created_by_adviser.last_name AS created_by_name,
                created_by_team.name AS created_by_team,
                modified_by_adviser.id AS modified_by_adviser_id,
                modified_by_adviser.first_name || ' ' || modified_by_adviser.last_name AS modified_by_name,
                modified_by_team.name AS modified_by_team,
                client_relationship_manager.id AS client_relationship_manager_id,
                client_relationship_manager.first_name || ' ' || client_relationship_manager.last_name AS client_relationship_manager_name,
                client_relationship_manager_team.name AS client_relationship_manager_team,
                project_manager.id AS project_manager_id,
                project_manager.first_name || ' ' || project_manager.last_name AS project_manager_name,
                project_manager_team.name AS project_manager_team,
                project_assurance_adviser.id AS project_assurance_adviser_id,
                project_assurance_adviser.first_name || ' ' || project_assurance_adviser.last_name AS project_assurance_adviser_name,
                project_assurance_adviser_team.name AS project_assurance_adviser_team,
                account_manager.id AS account_manager_id,
                account_manager.first_name || ' ' || account_manager.last_name AS account_manager_name,
                account_manager_team.name AS account_manager_team,

                --Show all other team members
                (
                    SELECT STRING_AGG(
                            CONCAT(data_hub__advisers.first_name, ' ', data_hub__advisers.last_name, ' (', teams_dataset.name,')'), '; ')
                    FROM dit.data_hub__advisers
                    JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                ) AS team_members,

                --Show all members of the full virtual team (project manager, project assurance adviser, client relationship manager, project creator, last modifier, account manager + all other team members)
                (
                    SELECT STRING_AGG(
                            CONCAT(data_hub__advisers.first_name, ' ', data_hub__advisers.last_name, ' (', teams_dataset.name,
                                   ')'), '; ')
                    FROM dit.data_hub__advisers
                         JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                       OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                  client_relationship_manager.id, project_manager.id,
                                                  project_assurance_adviser.id, account_manager.id)
                ) AS all_virtual_team_members,

                --Show all members of the full virtual team who belong to DIT HQ teams
                (
                    SELECT STRING_AGG(
                            CONCAT(data_hub__advisers.first_name, ' ', data_hub__advisers.last_name, ' (', teams_dataset.name,
                                   ')'), '; ')
                    FROM dit.data_hub__advisers
                         JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE (data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                            OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                       client_relationship_manager.id, project_manager.id,
                                                       project_assurance_adviser.id, account_manager.id))
                      AND teams_dataset.role IN ('DIT HQ Department', 'HQ Investment Team', 'Sector Team')
                ) AS dit_hq_team_members,

                --Show all members of the full virtual team who are in 'IST' teams
                (
                    SELECT STRING_AGG(
                            CONCAT(data_hub__advisers.first_name, ' ', data_hub__advisers.last_name, ' (', teams_dataset.name,
                                   ')'), '; ')
                    FROM dit.data_hub__advisers
                         JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE (data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                            OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                       client_relationship_manager.id, project_manager.id,
                                                       project_assurance_adviser.id, account_manager.id))
                      AND teams_dataset.name LIKE '%IST%'
                ) AS ist_team_members,

                --Show all overseas post teams in the full virtual team
                (
                    SELECT STRING_AGG(DISTINCT teams_dataset.name, '; ')
                    FROM dit.data_hub__advisers
                         JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE (data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                            OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                       client_relationship_manager.id, project_manager.id,
                                                       project_assurance_adviser.id, account_manager.id))
                      AND teams_dataset.role = 'Post'
                ) AS post_teams,

                --Show all investment delivery partners in the full virtual team
                (
                    SELECT STRING_AGG(DISTINCT ref_investment_delivery_partners_lep_da.delivery_partner_name, '; ')
                    FROM ref_investment_delivery_partners_lep_da
                         JOIN teams_dataset
                              ON teams_dataset.name = ref_investment_delivery_partners_lep_da.delivery_partner_name
                         JOIN dit.data_hub__advisers ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE (data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                            OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                       client_relationship_manager.id, project_manager.id,
                                                       project_assurance_adviser.id, account_manager.id))
                ) AS delivery_partner_teams,

                --Show all local enterprise partnerships and devolved administrations linked to members of the full virtual team
                CONCAT((
                    SELECT STRING_AGG(DISTINCT ref_investment_delivery_partners_lep_da.local_enterprise_partnership_name, '; ')
                    FROM ref_investment_delivery_partners_lep_da
                    JOIN teams_dataset ON teams_dataset.name = ref_investment_delivery_partners_lep_da.delivery_partner_name
                    JOIN dit.data_hub__advisers ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE (
                        data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                        OR
                        data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id, client_relationship_manager.id, project_manager.id, project_assurance_adviser.id, account_manager.id)
                    )
                    AND local_enterprise_partnership_name != ''
                ), (
                    SELECT STRING_AGG(DISTINCT ref_investment_delivery_partners_lep_da.devolved_administration_name, '; ')
                    FROM ref_investment_delivery_partners_lep_da
                    JOIN teams_dataset ON teams_dataset.name = ref_investment_delivery_partners_lep_da.delivery_partner_name
                    JOIN dit.data_hub__advisers ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE (
                        data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                        OR
                        data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id, client_relationship_manager.id, project_manager.id, project_assurance_adviser.id, account_manager.id)
                    )
               )) AS lep_or_da_in_team,

                --Show all super regions and devolved administrations linked to members of the full virtual team
                CONCAT((
                   SELECT STRING_AGG(DISTINCT ref_super_regions.super_region_name, '; ')
                   FROM ref_super_regions
                        JOIN ref_leps_to_super_regions
                             ON ref_leps_to_super_regions.super_region_name_id = ref_super_regions.id
                        JOIN ref_lep_names ON ref_lep_names.id = ref_leps_to_super_regions.lep_name_id
                        JOIN ref_investment_delivery_partners_lep_da
                             ON ref_investment_delivery_partners_lep_da.local_enterprise_partnership_code
                                     = ref_lep_names.field_01
                        JOIN teams_dataset
                             ON teams_dataset.name = ref_investment_delivery_partners_lep_da.delivery_partner_name
                        JOIN dit.data_hub__advisers ON data_hub__advisers.team_id = teams_dataset.id
                   WHERE (data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                           OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                      client_relationship_manager.id, project_manager.id,
                                                      project_assurance_adviser.id, account_manager.id))
               ), (
                   SELECT STRING_AGG(DISTINCT ref_investment_delivery_partners_lep_da.devolved_administration_name,
                                     '; ')
                   FROM ref_investment_delivery_partners_lep_da
                        JOIN teams_dataset
                             ON teams_dataset.name = ref_investment_delivery_partners_lep_da.delivery_partner_name
                        JOIN dit.data_hub__advisers ON data_hub__advisers.team_id = teams_dataset.id
                   WHERE (data_hub__advisers.id = ANY (investment_projects.team_member_ids::UUID[])
                           OR data_hub__advisers.id IN (created_by_adviser.id, modified_by_adviser.id,
                                                      client_relationship_manager.id, project_manager.id,
                                                      project_assurance_adviser.id, account_manager.id))
               ))
            AS super_region_or_da_in_team
            FROM investment_projects
                 JOIN dit.data_hub__companies investor_company ON investor_company.id = investment_projects.investor_company_id
                 LEFT JOIN dit.data_hub__advisers created_by_adviser ON created_by_adviser.id = investment_projects.created_by_id
                 LEFT JOIN teams_dataset created_by_team ON created_by_team.id = created_by_adviser.team_id
                 LEFT JOIN dit.data_hub__advisers modified_by_adviser ON modified_by_adviser.id = investment_projects.modified_by_id
                 LEFT JOIN teams_dataset modified_by_team ON modified_by_team.id = modified_by_adviser.team_id
                 LEFT JOIN dit.data_hub__advisers client_relationship_manager ON client_relationship_manager.id = investment_projects.client_relationship_manager_id
                 LEFT JOIN teams_dataset client_relationship_manager_team ON client_relationship_manager_team.id = client_relationship_manager.team_id
                 LEFT JOIN dit.data_hub__advisers project_manager ON project_manager.id = investment_projects.project_manager_id
                 LEFT JOIN teams_dataset project_manager_team ON project_manager_team.id = project_manager.team_id
                 LEFT JOIN dit.data_hub__advisers project_assurance_adviser ON project_assurance_adviser.id = investment_projects.project_assurance_adviser_id
                 LEFT JOIN teams_dataset project_assurance_adviser_team ON project_assurance_adviser_team.id = project_assurance_adviser.team_id
                 LEFT JOIN dit.data_hub__advisers account_manager ON account_manager.id = investor_company.one_list_account_owner_id
                 LEFT JOIN teams_dataset account_manager_team ON account_manager_team.id = account_manager.team_id
        )
    --Main query starts here
    SELECT
        investment_projects.id AS investment_project_id,
        investment_projects.project_reference AS project_reference_code,
        investment_projects.name AS project_name,
        investment_projects.description,
        investment_projects.actual_land_date,
        investment_projects.estimated_land_date,
        investment_projects.created_on::DATE,
        investment_projects.modified_on::DATE,
        latest_interactions.date_of_latest_interaction,
        investment_projects.fdi_value,
        investment_projects.investment_type,
        investment_projects.fdi_type,
        CASE
            WHEN (investment_projects.other_business_activity IS NULL OR investment_projects.other_business_activity = '') AND investment_projects.business_activities IS NOT NULL
                THEN ARRAY_TO_STRING(investment_projects.business_activities, '; ')
            WHEN investment_projects.other_business_activity IS NOT NULL AND investment_projects.business_activities IS NULL
                THEN investment_projects.other_business_activity
            WHEN investment_projects.other_business_activity IS NOT NULL AND investment_projects.business_activities IS NOT NULL
                THEN CONCAT(investment_projects.other_business_activity, ', ', ARRAY_TO_STRING(investment_projects.business_activities, '; '))
        END AS business_activities,
        investment_projects.investor_type,
        investment_projects.specific_programme,
        investment_projects.level_of_involvement,
        investment_projects.stage AS project_stage,
        investment_projects.status AS project_status,
        ARRAY_TO_STRING(investment_projects.competing_countries, ', ') AS competing_countries,
        investment_projects.referral_source_activity,
        investment_projects.referral_source_activity_website,
        investment_projects.referral_source_activity_marketing,
        investment_projects.total_investment,
        investment_projects.foreign_equity_investment,

        --Start Gross Value Added calculations
        CASE
            --Use existing Data Hub values for projects before April 2020
            WHEN investment_projects.actual_land_date < '2020-04-01'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date < '2020-04-01')
                THEN investment_projects.gross_value_added

            --Capital intensive projects after April 2020
            WHEN (investment_projects.actual_land_date > '2020-03-31'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date > '2020-03-31'))
                    AND ref_sectors_gva_value_bands.sector_classification = 'Capital intensive'
                THEN ROUND(
                    (ref_sectors_gva_value_bands.gva_multiplier * investment_projects.foreign_equity_investment)::NUMERIC,
                    2)

            --Labour intensive projects after April 2020
            WHEN (investment_projects.actual_land_date > '2020-03-31'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date > '2020-03-31'))
                    AND ref_sectors_gva_value_bands.sector_classification = 'Labour intensive'
                THEN ROUND((ref_sectors_gva_value_bands.gva_multiplier * investment_projects.number_new_jobs)::NUMERIC, 2)

        END AS gross_value_added,
        --End Gross Value Added calculations

        --Start assigning investment value bands
        CASE
            --For projects before April 2020
            WHEN investment_projects.actual_land_date < '2020-04-01'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date < '2020-04-01')
                THEN 'Not assigned'

            --Exception for Global Entrepreneur Programme projects
            WHEN (investment_projects.actual_land_date > '2020-03-31'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date > '2020-03-31'))
                    AND investment_projects.specific_programme = 'Global Entrepreneur Programme'
                THEN
                (CASE
                     WHEN investment_projects.number_new_jobs >= 42
                         THEN 'Value band A'
                     WHEN investment_projects.number_new_jobs >= 21
                         THEN 'Value band B'
                     WHEN investment_projects.number_new_jobs >= 11
                         THEN 'Value band C'
                     WHEN investment_projects.number_new_jobs >= 7
                         THEN 'Value band D'
                     WHEN investment_projects.number_new_jobs >= 1
                         THEN 'Value band E'
                 END)

            --For capital intensive projects after April 2020
            WHEN (investment_projects.actual_land_date > '2020-03-31'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date > '2020-03-31'))
                    AND ref_sectors_gva_value_bands.sector_classification = 'Capital intensive'
                THEN
                (
                    CASE
                        WHEN investment_projects.foreign_equity_investment
                                >= ref_sectors_gva_value_bands.value_band_a_minimum
                            THEN 'Value band A'
                        WHEN investment_projects.foreign_equity_investment
                                >= ref_sectors_gva_value_bands.value_band_b_minimum
                            THEN 'Value band B'
                        WHEN investment_projects.foreign_equity_investment
                                >= ref_sectors_gva_value_bands.value_band_c_minimum
                            THEN 'Value band C'
                        WHEN investment_projects.foreign_equity_investment
                                >= ref_sectors_gva_value_bands.value_band_d_minimum
                            THEN 'Value band D'
                        WHEN investment_projects.foreign_equity_investment
                                >= ref_sectors_gva_value_bands.value_band_e_minimum
                            THEN 'Value band E'
                    END)

            --For labour intensive projects after April 2020
            WHEN (investment_projects.actual_land_date > '2020-03-31'
                    OR (investment_projects.actual_land_date IS NULL
                        AND investment_projects.estimated_land_date > '2020-03-31'))
                    AND ref_sectors_gva_value_bands.sector_classification = 'Labour intensive'
                THEN
                (CASE
                     WHEN investment_projects.number_new_jobs >= ref_sectors_gva_value_bands.value_band_a_minimum
                         THEN 'Value band A'
                     WHEN investment_projects.number_new_jobs >= ref_sectors_gva_value_bands.value_band_b_minimum
                         THEN 'Value band B'
                     WHEN investment_projects.number_new_jobs >= ref_sectors_gva_value_bands.value_band_c_minimum
                         THEN 'Value band C'
                     WHEN investment_projects.number_new_jobs >= ref_sectors_gva_value_bands.value_band_d_minimum
                         THEN 'Value band D'
                     WHEN investment_projects.number_new_jobs >= ref_sectors_gva_value_bands.value_band_e_minimum
                         THEN 'Value band E'
                 END)
            ELSE 'Not assigned'
        END AS investment_value_band,
        --End assigning investment value bands

        investment_projects.government_assistance,
        investment_projects.r_and_d_budget,
        investment_projects.non_fdi_r_and_d_budget,
        investment_projects.new_tech_to_uk,
        investment_projects.number_new_jobs,
        investment_projects.number_safeguarded_jobs,
        investment_projects.number_new_jobs + investment_projects.number_safeguarded_jobs AS total_jobs,
        investment_projects.average_salary,
        investment_projects.client_requirements,
        investment_projects.anonymous_description,
        investor_company.address_country AS investor_company_country,
        ref_countries_territories_and_regions.region AS hmtc,
        investment_projects.sector AS original_project_sector,
        investment_projects.project_sector AS project_sector,
        ref_dit_sectors.field_03 AS dit_sector_cluster,

        --IST sector Cluster - to be added later
        --IST sector group - to be added later

        ARRAY_TO_STRING(investment_projects.possible_uk_regions, '; ') AS possible_uk_regions,
        ARRAY_TO_STRING(investment_projects.actual_uk_regions, '; ') AS actual_uk_regions,
        ARRAY_TO_STRING(investment_projects.delivery_partners, '; ') AS delivery_partners,

        --Super region (successes)
        CASE
            WHEN investment_projects.stage NOT IN ('Won', 'Verify win')
                THEN ''
            ELSE
                CONCAT((
                   SELECT STRING_AGG(DISTINCT ref_super_regions.super_region_name, '; ')
                   FROM ref_super_regions
                        JOIN ref_leps_to_super_regions
                             ON ref_leps_to_super_regions.super_region_name_id = ref_super_regions.id
                        JOIN ref_lep_names ON ref_lep_names.id = ref_leps_to_super_regions.lep_name_id
                        JOIN ref_investment_delivery_partners_lep_da
                             ON ref_investment_delivery_partners_lep_da.local_enterprise_partnership_code
                                     = ref_lep_names.field_01
                   WHERE ref_investment_delivery_partners_lep_da.delivery_partner_name
                           = ANY (investment_projects.delivery_partners::TEXT[])
               ), (
                   SELECT STRING_AGG(DISTINCT ref_investment_delivery_partners_lep_da.devolved_administration_name,
                                     '; ')
                   FROM ref_investment_delivery_partners_lep_da
                   WHERE ref_investment_delivery_partners_lep_da.delivery_partner_name
                           = ANY (investment_projects.delivery_partners::TEXT[])
               ))
        END AS super_region,
        investor_company.name AS investor_company_name,
        investor_company.company_number AS investor_company_comp_house_id,
        investor_company.headquarter_type,
        investor_company.one_list_tier AS investor_company_company_tier,
        investor_company.address_1,
        investor_company.address_2,
        investor_company.address_postcode,
        uk_company.name AS uk_company_name,
        uk_company.company_number AS uk_company_comp_house_id,
        uk_company.sector AS uk_company_sector,
        uk_company.address_1 AS uk_company_address_1,
        uk_company.address_2 AS uk_company_address_2,
        uk_company.uk_region AS uk_company_uk_region,
        project_team.created_by_name,
        project_team.created_by_team,
        project_team.modified_by_name,
        project_team.modified_by_team,
        project_team.client_relationship_manager_name,
        project_team.client_relationship_manager_team,
        project_team.project_manager_name,
        project_team.project_manager_team,
        project_team.project_assurance_adviser_name,
        project_team.project_assurance_adviser_team,
        project_team.account_manager_name,
        project_team.account_manager_team,
        project_team.team_members,
        project_team.all_virtual_team_members,
        project_team.dit_hq_team_members,
        project_team.ist_team_members,
        CASE
            WHEN project_team.post_teams LIKE '%;%'
                THEN 'Multiple'
            ELSE project_team.post_teams
        END AS post_team,
        project_team.delivery_partner_teams,
        project_team.lep_or_da_in_team,
        project_team.super_region_or_da_in_team,
        investment_projects.export_revenue
    FROM investment_projects
    JOIN dit.data_hub__companies investor_company ON investor_company.id = investment_projects.investor_company_id
    JOIN project_team ON project_team.project_id = investment_projects.id
    LEFT JOIN dit.data_hub__companies uk_company ON uk_company.id = investment_projects.uk_company_id
    LEFT JOIN ref_countries_territories_and_regions ON ref_countries_territories_and_regions.name = investor_company.address_country
    LEFT JOIN ref_dit_sectors ON ref_dit_sectors.full_sector_name = investment_projects.sector
    LEFT JOIN ref_sectors_gva_value_bands ON ref_sectors_gva_value_bands.full_sector_name = investment_projects.sector
    LEFT JOIN latest_interactions ON latest_interactions.investment_project_id = investment_projects.id
    '''
