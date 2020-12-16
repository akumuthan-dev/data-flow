import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from airflow.operators.python_operator import PythonOperator

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
    use_utc_now_as_source_modified = True
    dependencies = [
        CompaniesDatasetPipeline,
        ExportWinsAdvisersDatasetPipeline,
        ExportWinsHVCDatasetPipeline,
        ExportWinsWinsDatasetPipeline,
    ]

    table_config = TableConfig(
        schema='dit',
        table_name="export_wins__dashboard_data",
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
          WHEN data_hub__companies.id IS NOT NULL
            THEN CONCAT('https://datahub.trade.gov.uk/companies/',data_hub__companies.id,'/exports')
          ELSE ''
        END AS dh_company_link,
        CASE
          WHEN data_hub__companies.id IS NOT NULL
            THEN data_hub__companies.name
          ELSE ''
        END AS dh_company_name

        FROM export_wins_wins_dataset
          LEFT JOIN export_wins_wins_dataset_match_ids ON export_wins_wins_dataset_match_ids.id::uuid = export_wins_wins_dataset.id
          LEFT JOIN companies_dataset_match_ids ON companies_dataset_match_ids.match_id = export_wins_wins_dataset_match_ids.match_id
          LEFT JOIN dit.data_hub__companies ON data_hub__companies.id = companies_dataset_match_ids.id::uuid

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

    schedule_interval = '@daily'
    start_date = datetime.datetime(2020, 3, 3)

    def get_fetch_operator(self):
        op = PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[
                self.query,
                self.target_db,
                self.table_config.table_name,  # pylint: disable=no-member
            ],
        )
        return op
