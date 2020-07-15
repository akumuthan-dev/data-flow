"""Airflow Pipeline DAGs containing aggregate data for publishing on dashboards"""

from datetime import datetime

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

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

    def get_fetch_operator(self):
        return PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[self.query, self.target_db, self.table_config.table_name],
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
            ('project_land_date', sa.Column('project_land_date', sa.Date)),
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
        WITH projects_with_single_land_date AS (
            SELECT
                investment_projects_dataset.*,
                CASE
                    WHEN investment_projects_dataset.stage IN ('Prospect', 'Assign PM', 'Active')
                        THEN investment_projects_dataset.estimated_land_date
                    ELSE investment_projects_dataset.actual_land_date
                END AS project_land_date
            FROM investment_projects_dataset
        )
        SELECT
            projects_with_single_land_date.id,
            projects_with_single_land_date.project_reference,
            CONCAT('https://datahub.trade.gov.uk/investments/projects/', projects_with_single_land_date.id, '/details') AS project_link,
            projects_with_single_land_date.total_investment,
            projects_with_single_land_date.number_new_jobs,
            projects_with_single_land_date.number_safeguarded_jobs,
            projects_with_single_land_date.stage,
            projects_with_single_land_date.status::TEXT,
            projects_with_single_land_date.fdi_value,
            companies_dataset.address_country::TEXT AS investor_company_country,
            ref_hmtc_overseas_regions.overseas_region_name::TEXT AS overseas_region,
            SPLIT_PART(projects_with_single_land_date.sector, ' : ', 1) AS project_sector,
            ref_dit_sectors.field_03::TEXT AS sector_cluster,
            projects_with_single_land_date.project_land_date
        FROM projects_with_single_land_date
        JOIN companies_dataset ON companies_dataset.id = projects_with_single_land_date.investor_company_id
        JOIN ref_dit_sectors ON ref_dit_sectors.full_sector_name = projects_with_single_land_date.sector
        LEFT JOIN ref_countries_and_territories ON ref_countries_and_territories.country_or_territory_name = companies_dataset.address_country
        LEFT JOIN ref_countries_territories_overseas_regions ON ref_countries_territories_overseas_regions.field_02_id = ref_countries_and_territories.id
        LEFT JOIN ref_hmtc_overseas_regions ON ref_hmtc_overseas_regions.id = ref_countries_territories_overseas_regions.field_03_id
        WHERE projects_with_single_land_date.project_land_date BETWEEN '2020-04-01' AND '2021-03-31'
        AND projects_with_single_land_date.investment_type = 'FDI'
        AND projects_with_single_land_date.level_of_involvement != 'No Involvement'
        AND (projects_with_single_land_date.status = 'ongoing' OR projects_with_single_land_date.status = 'won')

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
    with covid_interactions as (select * from interactions_dataset
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
        select advisers_dataset.id as "id", teams_dataset.name as team, teams_dataset.role as role from advisers_dataset
        join teams_dataset on teams_dataset.id = advisers_dataset.team_id
        where advisers_dataset.id in (select unnest(covid_interactions.adviser_ids)::uuid from covid_interactions)
    ),
    c_contacts as (
        select contacts_dataset.id, contact_name from contacts_dataset
        where contacts_dataset.id in (select unnest(covid_interactions.contact_ids)::uuid from covid_interactions)
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
    join companies_dataset co on co.id = ci.company_id
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
            unnest(interactions_dataset.adviser_ids) AS adviser_id,
            interactions_dataset.*
        FROM interactions_dataset
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
            companies_dataset.name AS company_name,
            CONCAT('https://datahub.trade.gov.uk/companies/',companies_dataset.id,'/activity') AS company_link,
            companies_dataset.address_country AS company_country,
            companies_dataset.address_postcode AS company_postcode,
            CASE
                WHEN ons_postcodes.long IS NOT NULL
                THEN CONCAT(ons_postcodes.lat, ', ', ons_postcodes.long)
                END AS company_address_latitude_longitude,
            companies_dataset.uk_region AS company_uk_region,
            companies_dataset.number_of_employees AS company_number_of_employees,
            companies_dataset.turnover AS company_turnover,
            companies_dataset.one_list_tier AS company_one_list_tier,
            SPLIT_PART(companies_dataset.sector, ' : ', 1) AS company_sector,
            minister_interactions.interaction_date AS interaction_date,
            minister_interactions.adviser_id::UUID AS adviser_id,
            CONCAT(advisers_dataset.first_name,' ',advisers_dataset.last_name) AS adviser_name,
            minister_interactions.interaction_subject AS interaction_subject,
            minister_interactions.communication_channel AS communication_channel,
            array_to_string(minister_interactions.policy_areas, ', ') AS policy_areas,
            minister_interactions.policy_feedback_notes AS policy_feedback_notes,
            array_to_string(minister_interactions.policy_issue_types, ', ') AS policy_issue_types,
            CASE
                WHEN companies_dataset.duns_number IS NULL
                THEN 'No'
                ELSE 'Yes'
            END AS dun_and_bradstreet_linked_record,
            minister_interactions.service_delivery AS service
        FROM minister_interactions
        JOIN companies_dataset ON companies_dataset.id = minister_interactions.company_id
        JOIN advisers_dataset ON advisers_dataset.id = minister_interactions.adviser_id::uuid
        LEFT JOIN ons_postcodes ON REPLACE(ons_postcodes.pcd2,' ','') = REPLACE(companies_dataset.address_postcode, ' ','')
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
        ORDER BY companies_dataset.name ASC
    '''
