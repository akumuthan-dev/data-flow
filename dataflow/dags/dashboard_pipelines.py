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
    ExportWinsAdvisersDatasetPipeline,
    ExportWinsHVCDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
    InteractionsDatasetPipeline,
    InvestmentProjectsDatasetPipeline,
    ONSPostcodePipeline,
    TeamsDatasetPipeline,
)
from dataflow.operators.db_tables import query_database
from dataflow.utils import TableConfig

DB_SCHEMA = 'dashboard'


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
        schema=DB_SCHEMA,
        table_name="fdi",
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
        SELECT
          investment_projects_dataset.actual_land_date,
          investment_projects_dataset.actual_uk_regions,
          investment_projects_dataset.id,
          investment_projects_dataset.estimated_land_date,
          investment_projects_dataset.investment_type,
          investment_projects_dataset.level_of_involvement,
          investment_projects_dataset.number_new_jobs,
          investment_projects_dataset.number_safeguarded_jobs,
          investment_projects_dataset.possible_uk_regions,
          investment_projects_dataset.fdi_value,
          investment_projects_dataset.project_reference,
          investment_projects_dataset.investor_company_sector,
          investment_projects_dataset.stage,
          investment_projects_dataset.status,
          investment_projects_dataset.foreign_equity_investment,
          investment_projects_dataset.address_postcode,
          investment_projects_dataset.investor_company_id,
          address_country
        FROM investment_projects_dataset
        JOIN companies_dataset ON companies_dataset.id = investment_projects_dataset.investor_company_id
        WHERE investment_projects_dataset.actual_land_date BETWEEN '2020-04-01' AND '2021-03-31'
        AND investment_projects_dataset.estimated_land_date BETWEEN '2020-04-01' AND '2021-03-31'
        AND investment_projects_dataset.investment_type = 'FDI'
        AND investment_projects_dataset.status = 'ongoing'
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
        schema=DB_SCHEMA,
        table_name="coronavirus_interactions",
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
        schema=DB_SCHEMA,
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


class ExportWinsDashboardPipeline(_SQLPipelineDAG):
    schedule_interval = '@daily'
    start_date = datetime.datetime(2020, 3, 3)
    dependencies = [
        CompaniesDatasetPipeline,
        ExportWinsAdvisersDatasetPipeline,
        ExportWinsHVCDatasetPipeline,
        ExportWinsWinsDatasetPipeline,
    ]
    table_config = TableConfig(
        schema=DB_SCHEMA,
        table_name="export_wins",
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ("EW ID", sa.Column("ew_id", UUID)),
            ("Company name", sa.Column("company_name", sa.Text)),
            ("DH company link", sa.Column("dh_company_link", sa.Text)),
            ("EW agree with win", sa.Column("ew_agree_with_win", sa.Boolean)),
            ("EW confirmation created", sa.Column("ew_confirmation_created", sa.Date)),
            (
                "Confirmation financial year",
                sa.Column("confirmation_financial_year", sa.Text),
            ),
            ("EW country", sa.Column("ew_country", sa.Text)),
            ("EW created date", sa.Column("ew_created_date", sa.Date)),
            ("EW customer email date", sa.Column("ew_customer_email_date", sa.Date)),
            ("EW date business won", sa.Column("ew_date_business_won", sa.Date)),
            (
                "EW total expected export value",
                sa.Column("ew_total_expected_export_value", sa.BigInteger),
            ),
            (
                "EW total expected non-export value",
                sa.Column("ew_total_expected_non_export_value", sa.BigInteger),
            ),
            (
                "EW total expected ODI value",
                sa.Column("ew_total_expected_odi_value", sa.BigInteger),
            ),
            ("EW Customer Location", sa.Column("ew_customer_location", sa.Text)),
            ("EW sector", sa.Column("ew_sector", sa.Text)),
            ("Time to confirm", sa.Column("time_to_confirm", sa.Float)),
            ("EW HVC Code", sa.Column("ew_hvc_code", sa.Text)),
            ("EW HVC Name", sa.Column("ew_hvc_name", sa.Text)),
            ("participant_name", sa.Column("participant_name", sa.Text)),
            ("participant_team", sa.Column("participant_team", sa.Text)),
            ("team_type", sa.Column("team_type", sa.Text)),
            ("contribution_type", sa.Column("contribution_type", sa.Text)),
            (
                "Number of Advisers Involved",
                sa.Column("number_of_advisers_involved", sa.BigInteger),
            ),
        ],
    )

    query = '''
    WITH datahub_companies AS (
      SELECT
        DISTINCT ON (export_wins_wins_dataset.id) export_wins_wins_dataset.id AS export_win_id,
        CASE
          WHEN companies_dataset.id IS NOT NULL
            THEN CONCAT('https://datahub.trade.gov.uk/companies/',companies_dataset.id,'/exports')
          ELSE ''
        END AS dh_company_link,
        CASE
          WHEN companies_dataset.id IS NOT NULL
            THEN companies_dataset.name
          ELSE ''
        END AS dh_company_name

        FROM export_wins_wins_dataset
          LEFT JOIN export_wins_wins_dataset_match_ids ON export_wins_wins_dataset_match_ids.id::uuid = export_wins_wins_dataset.id
          LEFT JOIN companies_dataset_match_ids ON companies_dataset_match_ids.match_id = export_wins_wins_dataset_match_ids.match_id
          LEFT JOIN companies_dataset ON companies_dataset.id = companies_dataset_match_ids.id::uuid

    ), win_participants AS (
      select
        export_wins_wins_dataset.id AS win_id,
        initcap(export_wins_wins_dataset.lead_officer_name) AS participant_name,
        export_wins_wins_dataset.hq_team AS participant_team,
        export_wins_wins_dataset.team_type AS team_type,
        'Lead' AS contribution_type

      from export_wins_wins_dataset
        UNION ALL
          select
            export_wins_advisers_dataset.win_id,
            initcap(export_wins_advisers_dataset.name),
            export_wins_advisers_dataset.hq_team,
            export_wins_advisers_dataset.team_type,
            'Contributor' AS "Contribution"

          from export_wins_advisers_dataset

    ), contributor_count AS (
      SELECT
        export_wins_wins_dataset.id AS id,
        count(export_wins_advisers_dataset.*) AS contributor_count

      from export_wins_wins_dataset
        join export_wins_advisers_dataset on export_wins_advisers_dataset.win_id = export_wins_wins_dataset.id

      group by export_wins_wins_dataset.id

    )

    SELECT
      export_wins_wins_dataset.id AS "EW ID",
      CASE
        WHEN datahub_companies.dh_company_name = ''
          THEN export_wins_wins_dataset.company_name
        ELSE datahub_companies.dh_company_name
      END AS "Company name",
      datahub_companies.dh_company_link AS "DH company link",
      export_wins_wins_dataset.confirmation_agree_with_win AS "EW agree with win",
      CASE
        WHEN export_wins_wins_dataset.confirmation_agree_with_win = true
          THEN 'Verified'
        ELSE 'Unverified'
      END AS "Verified or unverified",
      export_wins_wins_dataset.confirmation_created::text AS "EW confirmation created",
      CASE
        WHEN export_wins_wins_dataset.confirmation_created IS NULL
          THEN NULL
        WHEN DATE_PART('month', export_wins_wins_dataset.confirmation_created) >= 4
          THEN CONCAT((DATE_PART('year', export_wins_wins_dataset.confirmation_created)::varchar),' / ',(DATE_PART('year', export_wins_wins_dataset.confirmation_created + interval '+1' year)::varchar))
        ELSE CONCAT((DATE_PART('year', export_wins_wins_dataset.confirmation_created + interval '-1' year)::varchar),' / ',(DATE_PART('year', export_wins_wins_dataset.confirmation_created)::varchar))
      END AS "Confirmation financial year",
      export_wins_wins_dataset.country AS "EW country",
      export_wins_wins_dataset.created::text AS "EW created date",
      export_wins_wins_dataset.customer_email_date::text AS "EW customer email date",
      export_wins_wins_dataset.date::text AS "EW date business won",
      export_wins_wins_dataset.total_expected_export_value AS "EW total expected export value",
      export_wins_wins_dataset.total_expected_non_export_value AS "EW total expected non-export value",
      export_wins_wins_dataset.total_expected_odi_value AS "EW total expected ODI value",
      export_wins_wins_dataset.customer_location AS "EW Customer Location",
      SPLIT_PART(export_wins_wins_dataset.sector, ' : ', 1) AS "EW sector",
      DATE_PART('day', export_wins_wins_dataset.confirmation_created - export_wins_wins_dataset.customer_email_date) AS "Time to confirm",
      LEFT(export_wins_wins_dataset.hvc,4) AS "EW HVC Code",
      export_wins_hvc_dataset.name AS "EW HVC Name",
      win_participants.participant_name,
      win_participants.participant_team,
      win_participants.team_type,
      win_participants.contribution_type,
      CASE WHEN contributor_count IS NULL THEN 1 ELSE (contributor_count.contributor_count + 1) END AS "Number of Advisers Involved"

    FROM export_wins_wins_dataset
      JOIN datahub_companies ON datahub_companies.export_win_id = export_wins_wins_dataset.id
      LEFT JOIN export_wins_hvc_dataset ON export_wins_wins_dataset.hvc = CONCAT(export_wins_hvc_dataset.campaign_id, export_wins_hvc_dataset.financial_year)
      JOIN win_participants ON win_participants.win_id = export_wins_wins_dataset.id
      LEFT JOIN contributor_count ON contributor_count.id = export_wins_wins_dataset.id
    '''
