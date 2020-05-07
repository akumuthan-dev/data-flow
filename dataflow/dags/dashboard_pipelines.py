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
            ("actual_land_date", sa.Column("actual_land_date", sa.Date)),
            ("actual_uk_regions", sa.Column("actual_uk_regions", sa.ARRAY(sa.Text))),
            ("id", sa.Column("id", UUID, primary_key=True)),
            ("estimated_land_date", sa.Column("estimated_land_date", sa.Date)),
            ("investment_type", sa.Column("investment_type", sa.Text)),
            ("level_of_involvement", sa.Column("level_of_involvement", sa.Text)),
            ("number_new_jobs", sa.Column("number_new_jobs", sa.Integer)),
            (
                "number_safeguarded_jobs",
                sa.Column("number_safeguarded_jobs", sa.Integer),
            ),
            (
                "possible_uk_regions",
                sa.Column("possible_uk_regions", sa.ARRAY(sa.Text)),
            ),
            ("fdi_value", sa.Column("fdi_value", sa.Text)),
            ("project_reference", sa.Column("project_reference", sa.Text)),
            (
                "investor_company_sector",
                sa.Column("investor_company_sector", sa.String),
            ),
            ("stage", sa.Column("stage", sa.Text)),
            ("status", sa.Column("status", sa.Text)),
            (
                "foreign_equity_investment",
                sa.Column("foreign_equity_investment", sa.Float),
            ),
            ("address_postcode", sa.Column("address_postcode", sa.String)),
            ("investor_company_id", sa.Column("investor_company_id", UUID)),
            ("address_country", sa.Column("address_country", sa.String)),
        ],
    )

    query = '''
    select
      investment_projects_dataset.actual_land_date::text,
      investment_projects_dataset.actual_uk_regions::text,
      investment_projects_dataset.id::text,
      investment_projects_dataset.estimated_land_date::text,
      investment_projects_dataset.investment_type::text,
      investment_projects_dataset.level_of_involvement::text,
      investment_projects_dataset.number_new_jobs::text,
      investment_projects_dataset.number_safeguarded_jobs::text,
      investment_projects_dataset.possible_uk_regions::text,
      investment_projects_dataset.fdi_value::text,
      investment_projects_dataset.project_reference::text,
      investment_projects_dataset.investor_company_sector::text,
      investment_projects_dataset.stage::text,
      investment_projects_dataset.status::text,
      investment_projects_dataset.foreign_equity_investment::text,
      investment_projects_dataset.address_postcode::text,
      investment_projects_dataset.investor_company_id::text,
      address_country::text

    from investment_projects_dataset join companies_dataset
      on companies_dataset.id = investment_projects_dataset.investor_company_id

    where investment_projects_dataset.actual_land_date between '2020-04-01' AND '2021-03-31'
     and investment_projects_dataset.estimated_land_date between '2020-04-01' AND '2021-03-31'
     and investment_projects_dataset.investment_type = 'FDI'
     and investment_projects_dataset.status = 'ongoing'
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
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('Interaction ID', sa.Column('Interaction ID', UUID)),
            ('Interaction Link', sa.Column('Interaction Link', sa.Text)),
            ('Company Name', sa.Column('Company Name', sa.Text)),
            ('Company Link', sa.Column('Company Link', sa.Text)),
            ('Company Country', sa.Column('Company Country', sa.Text)),
            ('Company Postcode', sa.Column('Company Postcode', sa.Text)),
            (
                'Company Address Latitude and Longitude',
                sa.Column('Company Address Latitude and Longitude', sa.Text),
            ),
            ('Company UK Region', sa.Column('Company UK Region', sa.Text)),
            (
                'Company number of employees',
                sa.Column('Company number of employees', sa.Text),
            ),
            ('Company Turnover', sa.Column('Company Turnover', sa.Text)),
            ('Company One List Tier', sa.Column('Company One List Tier', sa.Text)),
            ('Company Sector', sa.Column('Company Sector', sa.Text)),
            ('Interaction Date', sa.Column('Interaction Date', sa.Date)),
            ('Adviser ID', sa.Column('Adviser ID', UUID)),
            ('Adviser Name', sa.Column('Adviser Name', sa.Text)),
            ('Interaction Subject', sa.Column('Interaction Subject', sa.Text)),
            ('Communication Channel', sa.Column('Communication Channel', sa.Text)),
            ('Policy Areas', sa.Column('Policy Areas', sa.Text)),
            ('Policy Feedback Notes', sa.Column('Policy Feedback Notes', sa.Text)),
            ('Policy Issue Types', sa.Column('Policy Issue Types', sa.Text)),
            (
                'Dun and Bradstreet Linked Record',
                sa.Column('Dun and Bradstreet Linked Record', sa.Text),
            ),
            ('Service', sa.Column('Service', sa.Text)),
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
            CONCAT(minister_interactions.id, '_', minister_interactions.adviser_id) AS id,
            minister_interactions.id AS "Interaction ID",
            minister_interactions.interaction_link AS "Interaction Link",
            companies_dataset.name AS "Company Name",
            CONCAT('https://datahub.trade.gov.uk/companies/',companies_dataset.id,'/activity') AS "Company Link",
            companies_dataset.address_country AS "Company Country",
            companies_dataset.address_postcode AS "Company Postcode",
            CASE
                WHEN ons_postcodes.long IS NOT NULL
                THEN CONCAT(ons_postcodes.lat, ', ', ons_postcodes.long)
                END AS "Company Address Latitude and Longitude",
            companies_dataset.uk_region AS "Company UK Region",
            companies_dataset.number_of_employees AS "Company number of employees",
            companies_dataset.turnover AS "Company Turnover",
            companies_dataset.one_list_tier AS "Company One List Tier",
            SPLIT_PART(companies_dataset.sector, ' : ', 1) AS "Company Sector",
            minister_interactions.interaction_date AS "Interaction Date",
            minister_interactions.adviser_id::UUID AS "Adviser ID",
            CONCAT(advisers_dataset.first_name,' ',advisers_dataset.last_name) AS "Adviser Name",
            minister_interactions.interaction_subject AS "Interaction Subject",
            minister_interactions.communication_channel AS "Communication Channel",
            array_to_string(minister_interactions.policy_areas, ', ') AS "Policy Areas",
            minister_interactions.policy_feedback_notes AS "Policy Feedback Notes",
            array_to_string(minister_interactions.policy_issue_types, ', ') AS "Policy Issue Types",
            CASE
                WHEN companies_dataset.duns_number IS NULL
                THEN 'No'
                ELSE 'Yes'
            END AS "Dun and Bradstreet Linked Record",
            minister_interactions.service_delivery AS "Service"
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
