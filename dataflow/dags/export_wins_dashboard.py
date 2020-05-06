import datetime

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow.dags import _PipelineDAG
from dataflow.operators.db_tables import query_database

from dataflow.dags.dataset_pipelines import (
    CompaniesDatasetPipeline,
    ExportWinsAdvisersDatasetPipeline,
    ExportWinsHVCDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
)
from dataflow.utils import TableConfig


class ExportWinsDashboardPipeline(_PipelineDAG):
    dependencies = [
        CompaniesDatasetPipeline,
        ExportWinsAdvisersDatasetPipeline,
        ExportWinsHVCDatasetPipeline,
        ExportWinsWinsDatasetPipeline,
    ]

    table_config = TableConfig(
        table_name="export_wins_dashboard_data",
        field_mapping=[
            ("EW ID", sa.Column("EW ID", UUID)),
            ("Company name", sa.Column("Company Name", sa.Text)),
            ("DH company link", sa.Column("DH company_link", sa.Text)),
            ("EW agree with win", sa.Column("EW agree with win", sa.Boolean)),
            ("EW confirmation created", sa.Column("EW confirmation created", sa.Date)),
            (
                "Confirmation financial year",
                sa.Column("Confirmation financial year", sa.Text),
            ),
            ("EW country", sa.Column("EW country", sa.Text)),
            ("EW created date", sa.Column("EW created date", sa.Date)),
            ("EW customer email date", sa.Column("EW customer email date", sa.Date)),
            ("EW date business won", sa.Column("EW date business won", sa.Date)),
            (
                "EW total expected export value",
                sa.Column("EW total expected export value", sa.BigInteger),
            ),
            (
                "EW total expected non-export value",
                sa.Column("EW total expected non-export value", sa.BigInteger),
            ),
            (
                "EW total expected ODI value",
                sa.Column("EW total expected ODI value", sa.BigInteger),
            ),
            ("EW Customer Location", sa.Column("EW Customer Location", sa.Text)),
            ("EW sector", sa.Column("EW sector", sa.Text)),
            ("Time to confirm", sa.Column("Time to confirm", sa.Float)),
            ("EW HVC Code", sa.Column("EW HVC Code", sa.Text)),
            ("EW HVC Name", sa.Column("EW HVC Name", sa.Text)),
            ("participant_name", sa.Column("participant_name", sa.Text)),
            ("participant_team", sa.Column("participant_team", sa.Text)),
            ("team_type", sa.Column("team_type", sa.Text)),
            ("contribution_type", sa.Column("contribution_type", sa.Text)),
            (
                "Number of Advisers Involved",
                sa.Column("Number of Advisers Involved", sa.BigInteger),
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
      export_wins_wins_dataset.id::text AS "EW ID",
      CASE
        WHEN datahub_companies.dh_company_name = ''
          THEN export_wins_wins_dataset.company_name::text
        ELSE datahub_companies.dh_company_name::text
      END AS "Company name",
      datahub_companies.dh_company_link::text AS "DH company link",
      export_wins_wins_dataset.confirmation_agree_with_win::text AS "EW agree with win",
      CASE
        WHEN export_wins_wins_dataset.confirmation_agree_with_win = true
          THEN 'Verified'
        ELSE 'Unverified'
      END AS "Verified or unverified",
      export_wins_wins_dataset.confirmation_created::date::text AS "EW confirmation created",
      CASE
        WHEN export_wins_wins_dataset.confirmation_created IS NULL
          THEN NULL
        WHEN DATE_PART('month', export_wins_wins_dataset.confirmation_created) >= 4
          THEN CONCAT((DATE_PART('year', export_wins_wins_dataset.confirmation_created)::varchar),' / ',(DATE_PART('year', export_wins_wins_dataset.confirmation_created + interval '+1' year)::varchar))::text
        ELSE CONCAT((DATE_PART('year', export_wins_wins_dataset.confirmation_created + interval '-1' year)::varchar),' / ',(DATE_PART('year', export_wins_wins_dataset.confirmation_created)::varchar))::text
      END AS "Confirmation financial year",
      export_wins_wins_dataset.country::text AS "EW country",
      export_wins_wins_dataset.created::date::text AS "EW created date",
      export_wins_wins_dataset.customer_email_date::date::text AS "EW customer email date",
      export_wins_wins_dataset.date::text AS "EW date business won",
      export_wins_wins_dataset.total_expected_export_value::text AS "EW total expected export value",
      export_wins_wins_dataset.total_expected_non_export_value::text AS "EW total expected non-export value",
      export_wins_wins_dataset.total_expected_odi_value::text AS "EW total expected ODI value",
      export_wins_wins_dataset.customer_location::text AS "EW Customer Location",
      SPLIT_PART(export_wins_wins_dataset.sector, ' : ', 1) ::text AS "EW sector",
      DATE_PART('day', export_wins_wins_dataset.confirmation_created - export_wins_wins_dataset.customer_email_date)::text AS "Time to confirm",
      LEFT(export_wins_wins_dataset.hvc,4)::text AS "EW HVC Code",
      export_wins_hvc_dataset.name::text AS "EW HVC Name",
      win_participants.participant_name::text,
      win_participants.participant_team::text,
      win_participants.team_type::text,
      win_participants.contribution_type::text,
      CASE WHEN contributor_count IS NULL THEN 1 ELSE (contributor_count.contributor_count + 1) END AS "Number of Advisers Involved"

    FROM export_wins_wins_dataset
      JOIN datahub_companies ON datahub_companies.export_win_id = export_wins_wins_dataset.id
      LEFT JOIN export_wins_hvc_dataset ON export_wins_wins_dataset.hvc = CONCAT(export_wins_hvc_dataset.campaign_id, export_wins_hvc_dataset.financial_year)
      JOIN win_participants ON win_participants.win_id = export_wins_wins_dataset.id
      LEFT JOIN contributor_count ON contributor_count.id = export_wins_wins_dataset.id

    '''

    schedule_interval = '@daily'
    start_date = datetime.datetime(2020, 3, 3)

    def get_fetch_operator(self):
        op = PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[self.query, self.target_db, self.table_config.table_name],
        )
        return op