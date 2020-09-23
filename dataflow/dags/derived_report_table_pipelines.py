"""Airflow Pipeline DAGs that create derived tables to simplify reporting on data workspace"""
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from airflow.operators.python_operator import PythonOperator

from dataflow.dags.base import _PipelineDAG
from dataflow.dags.dataset_pipelines import (
    ExportWinsAdvisersDatasetPipeline,
    ExportWinsBreakdownsDatasetPipeline,
    ExportWinsHVCDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
)
from dataflow.operators.db_tables import query_database
from dataflow.utils import TableConfig


class _DerivedReportTablePipeline(_PipelineDAG):
    schedule_interval = '@daily'
    query: str
    allow_null_columns = True

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


class ExportWinsDerivedReportTablePipeline(_DerivedReportTablePipeline):
    dependencies = [
        ExportWinsAdvisersDatasetPipeline,
        ExportWinsBreakdownsDatasetPipeline,
        ExportWinsHVCDatasetPipeline,
        ExportWinsWinsDatasetPipeline,
    ]
    start_date = datetime(2020, 9, 16)
    table_config = TableConfig(
        schema='dit',
        table_name='export_wins__wins__derived',
        field_mapping=[
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('user_name', sa.Column('user_name', sa.Text)),
            ('user_email', sa.Column('user_email', sa.Text)),
            ('company_name', sa.Column('company_name', sa.Text)),
            ('cdms_reference', sa.Column('cdms_reference', sa.Text)),
            ('customer_name', sa.Column('customer_name', sa.Text)),
            ('customer_job_title', sa.Column('customer_job_title', sa.Text)),
            ('customer_email_address', sa.Column('customer_email_address', sa.Text)),
            ('customer_location', sa.Column('customer_location', sa.Text)),
            ('business_type', sa.Column('business_type', sa.Text)),
            ('description', sa.Column('description', sa.Text)),
            ('name_of_customer', sa.Column('name_of_customer', sa.Text)),
            ('name_of_export', sa.Column('name_of_export', sa.Text)),
            ('date_business_won', sa.Column('date_business_won', sa.Text)),
            ('country', sa.Column('country', sa.Text)),
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
            ('goods_vs_services', sa.Column('goods_vs_services', sa.Text)),
            ('sector', sa.Column('sector', sa.Text)),
            ('prosperity_fund_related', sa.Column('prosperity_fund_related', sa.Text)),
            ('hvc_code', sa.Column('hvc_code', sa.Text)),
            ('hvo_programme', sa.Column('hvo_programme', sa.Text)),
            (
                'has_hvo_specialist_involvement',
                sa.Column('has_hvo_specialist_involvement', sa.Text),
            ),
            ('is_e_exported', sa.Column('is_e_exported', sa.Text)),
            ('type_of_support_1', sa.Column('type_of_support_1', sa.Text)),
            ('type_of_support_2', sa.Column('type_of_support_2', sa.Text)),
            ('type_of_support_3', sa.Column('type_of_support_3', sa.Text)),
            ('associated_programme_1', sa.Column('associated_programme_1', sa.Text)),
            ('associated_programme_2', sa.Column('associated_programme_2', sa.Text)),
            ('associated_programme_3', sa.Column('associated_programme_3', sa.Text)),
            ('associated_programme_4', sa.Column('associated_programme_4', sa.Text)),
            ('associated_programme_5', sa.Column('associated_programme_5', sa.Text)),
            ('is_personally_confirmed', sa.Column('is_personally_confirmed', sa.Text)),
            (
                'is_line_manager_confirmed',
                sa.Column('is_line_manager_confirmed', sa.Text),
            ),
            ('lead_officer_name', sa.Column('lead_officer_name', sa.Text)),
            (
                'lead_officer_email_address',
                sa.Column('lead_officer_email_address', sa.Text),
            ),
            (
                'other_official_email_address',
                sa.Column('other_official_email_address', sa.Text),
            ),
            ('line_manager_name', sa.Column('line_manager_name', sa.Text)),
            ('team_type', sa.Column('team_type', sa.Text)),
            ('hq_team', sa.Column('hq_team', sa.Text)),
            ('business_potential', sa.Column('business_potential', sa.Text)),
            ('export_experience', sa.Column('export_experience', sa.Text)),
            ('created', sa.Column('created', sa.Text)),
            ('audit', sa.Column('audit', sa.Text)),
            ('advisers', sa.Column('advisers', sa.Text)),
            ('customer_email_sent', sa.Column('customer_email_sent', sa.Text)),
            ('customer_email_date', sa.Column('customer_email_date', sa.Text)),
            ('export_breakdown_1', sa.Column('export_breakdown_1', sa.Text)),
            ('export_breakdown_2', sa.Column('export_breakdown_2', sa.Text)),
            ('export_breakdown_3', sa.Column('export_breakdown_3', sa.Text)),
            ('export_breakdown_4', sa.Column('export_breakdown_4', sa.Text)),
            ('export_breakdown_5', sa.Column('export_breakdown_5', sa.Text)),
            ('non_export_breakdown_1', sa.Column('non_export_breakdown_1', sa.Text)),
            ('non_export_breakdown_2', sa.Column('non_export_breakdown_2', sa.Text)),
            ('non_export_breakdown_3', sa.Column('non_export_breakdown_3', sa.Text)),
            ('non_export_breakdown_4', sa.Column('non_export_breakdown_4', sa.Text)),
            ('non_export_breakdown_5', sa.Column('non_export_breakdown_5', sa.Text)),
            ('odi_breakdown_1', sa.Column('odi_breakdown_1', sa.Text)),
            ('odi_breakdown_2', sa.Column('odi_breakdown_2', sa.Text)),
            ('odi_breakdown_3', sa.Column('odi_breakdown_3', sa.Text)),
            ('odi_breakdown_4', sa.Column('odi_breakdown_4', sa.Text)),
            ('odi_breakdown_5', sa.Column('odi_breakdown_5', sa.Text)),
            (
                'customer_response_received',
                sa.Column('customer_response_received', sa.Text),
            ),
            ('date_response_received', sa.Column('date_response_received', sa.Text)),
            ('confirmation_name', sa.Column('confirmation_name', sa.Text)),
            ('agree_with_win', sa.Column('agree_with_win', sa.Text)),
            ('confirmation_comments', sa.Column('confirmation_comments', sa.Text)),
            (
                'confirmation_our_support',
                sa.Column('confirmation_our_support', sa.Text),
            ),
            (
                'confirmation_access_to_contacts',
                sa.Column('confirmation_access_to_contacts', sa.Text),
            ),
            (
                'confirmation_access_to_information',
                sa.Column('confirmation_access_to_information', sa.Text),
            ),
            (
                'confirmation_improved_profile',
                sa.Column('confirmation_improved_profile', sa.Text),
            ),
            (
                'confirmation_gained_confidence',
                sa.Column('confirmation_gained_confidence', sa.Text),
            ),
            (
                'confirmation_developed_relationships',
                sa.Column('confirmation_developed_relationships', sa.Text),
            ),
            (
                'confirmation_overcame_problem',
                sa.Column('confirmation_overcame_problem', sa.Text),
            ),
            (
                'confirmation_involved_state_enterprise',
                sa.Column('confirmation_involved_state_enterprise', sa.Text),
            ),
            (
                'confirmation_interventions_were_prerequisite',
                sa.Column('confirmation_interventions_were_prerequisite', sa.Text),
            ),
            (
                'confirmation_support_improved_speed',
                sa.Column('confirmation_support_improved_speed', sa.Text),
            ),
            (
                'confirmation_portion_without_help',
                sa.Column('confirmation_portion_without_help', sa.Text),
            ),
            (
                'confirmation_last_export',
                sa.Column('confirmation_last_export', sa.Text),
            ),
            (
                'confirmation_company_was_at_risk_of_not_exporting',
                sa.Column('confirmation_company_was_at_risk_of_not_exporting', sa.Text),
            ),
            (
                'confirmation_has_explicit_export_plans',
                sa.Column('confirmation_has_explicit_export_plans', sa.Text),
            ),
            (
                'confirmation_has_enabled_expansion_into_new_market',
                sa.Column(
                    'confirmation_has_enabled_expansion_into_new_market', sa.Text
                ),
            ),
            (
                'confirmation_has_increased_exports_as_percent_of_turnover',
                sa.Column(
                    'confirmation_has_increased_exports_as_percent_of_turnover', sa.Text
                ),
            ),
            (
                'confirmation_has_enabled_expansion_into_existing_market',
                sa.Column(
                    'confirmation_has_enabled_expansion_into_existing_market', sa.Text
                ),
            ),
            (
                'confirmation_case_study_willing',
                sa.Column('confirmation_case_study_willing', sa.Text),
            ),
            (
                'confirmation_marketing_source',
                sa.Column('confirmation_marketing_source', sa.Text),
            ),
            (
                'confirmation_other_marketing_source',
                sa.Column('confirmation_other_marketing_source', sa.Text),
            ),
            # Filtering columns
            ('country_code', sa.Column('country_code', sa.Text)),
            ('win_date', sa.Column('win_date', sa.Date)),
            ('win_financial_year', sa.Column('win_financial_year', sa.Integer)),
            (
                'confirmation_created_date',
                sa.Column('confirmation_created_date', sa.Date),
            ),
            (
                'confirmation_created_financial_year',
                sa.Column('confirmation_created_financial_year', sa.Integer),
            ),
            ('current_financial_year', sa.Column('current_financial_year', sa.Integer)),
        ],
    )
    query = '''
        WITH export_wins AS (
            SELECT
                *,
                CASE
                    WHEN EXTRACT('month' FROM date)::int >= 4
                    THEN (to_char(date, 'YYYY')::int)
                    ELSE (to_char(date + interval '-1' year, 'YYYY')::int)
                END as win_financial_year
            FROM export_wins_wins_dataset
            WHERE export_wins_wins_dataset.customer_email_date IS NOT NULL
        ), export_breakdowns AS (
            SELECT win_id, year, value
            FROM export_wins_breakdowns_dataset
            WHERE win_id IN (select id from export_wins)
            AND export_wins_breakdowns_dataset.type = 'Export'
        ), non_export_breakdowns AS (
            SELECT win_id, year, value
            FROM export_wins_breakdowns_dataset
            WHERE win_id IN (select id from export_wins)
            AND export_wins_breakdowns_dataset.type = 'Non-export'
        ), odi_breakdowns AS (
            SELECT win_id, year, value
            FROM export_wins_breakdowns_dataset
            WHERE win_id IN (select id from export_wins)
            AND export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
        ), contributing_advisers AS (
            SELECT win_id, STRING_AGG(CONCAT('Name: ', name, ', Team: ', team_type, ' - ', hq_team, ' - ', location), ', ') as advisers
            FROM export_wins_advisers_dataset
            GROUP BY 1
        )
        SELECT
            export_wins.id,
            CONCAT(export_wins.user_name, ' <', export_wins.user_email, '>') AS user_name,
            export_wins.user_email,
            export_wins.company_name,
            export_wins.cdms_reference,
            export_wins.customer_name,
            export_wins.customer_job_title,
            export_wins.customer_email_address,
            export_wins.customer_location,
            export_wins.business_type,
            export_wins.description,
            export_wins.name_of_customer,
            export_wins.name_of_export,
            to_char(export_wins.date, 'DD/MM/YYYY') AS date_business_won,
            export_wins.country,
            COALESCE(export_wins.total_expected_export_value, 0) AS total_expected_export_value,
            COALESCE(export_wins.total_expected_non_export_value, 0) AS total_expected_non_export_value,
            COALESCE(export_wins.total_expected_odi_value, 0) AS total_expected_odi_value,
            export_wins.goods_vs_services,
            export_wins.sector,
            COALESCE(export_wins.is_prosperity_fund_related, 'False') AS prosperity_fund_related,
            export_wins_hvc_dataset.name AS hvc_code,
            export_wins.hvo_programme,
            COALESCE(export_wins.has_hvo_specialist_involvement, 'False') AS has_hvo_specialist_involvement,
            COALESCE(export_wins.is_e_exported, 'False') AS is_e_exported,
            export_wins.type_of_support_1,
            export_wins.type_of_support_2,
            export_wins.type_of_support_3,
            export_wins.associated_programme_1,
            export_wins.associated_programme_2,
            export_wins.associated_programme_3,
            export_wins.associated_programme_4,
            export_wins.associated_programme_5,
            export_wins.is_personally_confirmed,
            export_wins.is_line_manager_confirmed,
            export_wins.lead_officer_name,
            export_wins.lead_officer_email_address,
            export_wins.other_official_email_address,
            export_wins.line_manager_name,
            export_wins.team_type,
            export_wins.hq_team,
            export_wins.business_potential,
            export_wins.export_experience,
            to_char(export_wins.created, 'DD/MM/YYYY') AS created,
            export_wins.audit,
            contributing_advisers.advisers,
            CASE WHEN export_wins.customer_email_date IS NOT NULL
            THEN 'Yes'
            ELSE 'No'
            END AS customer_email_sent,
            to_char(export_wins.customer_email_date, 'DD/MM/YYYY') AS customer_email_date,
            CONCAT(win_financial_year, ': £', COALESCE(ebd1.value, 0)) AS export_breakdown_1,
            CONCAT(win_financial_year + 1, ': £', COALESCE(ebd2.value, 0)) AS export_breakdown_2,
            CONCAT(win_financial_year + 2, ': £', COALESCE(ebd3.value, 0)) AS export_breakdown_3,
            CONCAT(win_financial_year + 3, ': £', COALESCE(ebd4.value, 0)) AS export_breakdown_4,
            CONCAT(win_financial_year + 4, ': £', COALESCE(ebd5.value, 0)) AS export_breakdown_5,
            CONCAT(win_financial_year, ': £', COALESCE(nebd1.value, 0)) AS non_export_breakdown_1,
            CONCAT(win_financial_year + 1, ': £', COALESCE(nebd2.value, 0)) AS non_export_breakdown_2,
            CONCAT(win_financial_year + 2, ': £', COALESCE(nebd3.value, 0)) AS non_export_breakdown_3,
            CONCAT(win_financial_year + 3, ': £', COALESCE(nebd4.value, 0)) AS non_export_breakdown_4,
            CONCAT(win_financial_year + 4, ': £', COALESCE(nebd5.value, 0)) AS non_export_breakdown_5,
            CONCAT(win_financial_year, ': £', COALESCE(odibd1.value, 0)) AS odi_breakdown_1,
            CONCAT(win_financial_year + 1, ': £', COALESCE(odibd2.value, 0)) AS odi_breakdown_2,
            CONCAT(win_financial_year + 2, ': £', COALESCE(odibd3.value, 0)) AS odi_breakdown_3,
            CONCAT(win_financial_year + 3, ': £', COALESCE(odibd4.value, 0)) AS odi_breakdown_4,
            CONCAT(win_financial_year + 4, ': £', COALESCE(odibd5.value, 0)) AS odi_breakdown_5,
            CASE WHEN export_wins.confirmation_created IS NOT NULL
            THEN 'Yes'
            ELSE 'No'
            END AS customer_response_received,
            to_char(export_wins.confirmation_created, 'DD/MM/YYYY') AS date_response_received,
            export_wins.confirmation_name,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_agree_with_win
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS agree_with_win,
            export_wins.confirmation_comments,
            export_wins.confirmation_our_support,
            export_wins.confirmation_access_to_contacts,
            export_wins.confirmation_access_to_information,
            export_wins.confirmation_improved_profile,
            export_wins.confirmation_gained_confidence,
            export_wins.confirmation_developed_relationships,
            export_wins.confirmation_overcame_problem,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_involved_state_enterprise
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_involved_state_enterprise,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_interventions_were_prerequisite
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_interventions_were_prerequisite,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_support_improved_speed
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_support_improved_speed,
            export_wins.confirmation_portion_without_help,
            export_wins.confirmation_last_export,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_company_was_at_risk_of_not_exporting
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_company_was_at_risk_of_not_exporting,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_explicit_export_plans
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_has_explicit_export_plans,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_new_market
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_has_enabled_expansion_into_new_market,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_increased_exports_as_percent_of_turnover
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_has_increased_exports_as_percent_of_turnover,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_existing_market
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_has_enabled_expansion_into_existing_market,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_case_study_willing
                THEN 'Yes'
                WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'No'
            END AS confirmation_case_study_willing,
            export_wins.confirmation_marketing_source,
            export_wins.confirmation_other_marketing_source,

            -- Used for filtering only
            export_wins.country_code,
            export_wins.date as win_date,
            export_wins.win_financial_year,
            export_wins.confirmation_created as confirmation_created_date,
            CASE
                WHEN EXTRACT('month' FROM export_wins.confirmation_created)::int >= 4
                THEN (to_char(export_wins.confirmation_created, 'YYYY')::int)
                ELSE (to_char(export_wins.confirmation_created + interval '-1' year, 'YYYY')::int)
            END as confirmation_created_financial_year,
            CASE
                WHEN EXTRACT('month' FROM CURRENT_DATE)::int >= 4
                THEN (to_char(CURRENT_DATE, 'YYYY')::int)
                ELSE (to_char(CURRENT_DATE + interval '-1' year, 'YYYY')::int)
            END as current_financial_year
        FROM export_wins
        LEFT JOIN export_breakdowns ebd1 ON (
            export_wins.id = ebd1.win_id
            AND ebd1.year = win_financial_year
        )
        LEFT JOIN export_breakdowns ebd2 ON (
            export_wins.id = ebd2.win_id
            AND ebd2.year = win_financial_year + 1
        )
        LEFT JOIN export_breakdowns ebd3 ON (
            export_wins.id = ebd3.win_id
            AND ebd3.year = win_financial_year + 2
        )
        LEFT JOIN export_breakdowns ebd4 ON (
            export_wins.id = ebd4.win_id
            AND ebd4.year = win_financial_year + 3
        )
        LEFT JOIN export_breakdowns ebd5 ON (
            export_wins.id = ebd5.win_id
            AND ebd5.year = win_financial_year + 4
        )
        LEFT JOIN non_export_breakdowns nebd1 ON (
            export_wins.id = nebd1.win_id
            AND nebd1.year = win_financial_year
        )
        LEFT JOIN non_export_breakdowns nebd2 ON (
            export_wins.id = nebd2.win_id
            AND nebd2.year = win_financial_year + 1
        )
        LEFT JOIN non_export_breakdowns nebd3 ON (
            export_wins.id = nebd3.win_id
            AND nebd3.year = win_financial_year + 2
        )
        LEFT JOIN non_export_breakdowns nebd4 ON (
            export_wins.id = nebd4.win_id
            AND nebd4.year = win_financial_year + 3
        )
        LEFT JOIN non_export_breakdowns nebd5 ON (
            export_wins.id = nebd5.win_id
            AND nebd5.year = win_financial_year + 4
        )
        LEFT JOIN odi_breakdowns odibd1 ON (
            export_wins.id = odibd1.win_id
            AND odibd1.year = win_financial_year
        )
        LEFT JOIN odi_breakdowns odibd2 ON (
            export_wins.id = odibd2.win_id
            AND odibd2.year = win_financial_year + 1
        )
        LEFT JOIN odi_breakdowns odibd3 ON (
            export_wins.id = odibd3.win_id
            AND odibd3.year = win_financial_year + 2
        )
        LEFT JOIN odi_breakdowns odibd4 ON (
            export_wins.id = odibd4.win_id
            AND odibd4.year = win_financial_year + 3
        )
        LEFT JOIN odi_breakdowns odibd5 ON (
            export_wins.id = odibd5.win_id
            AND odibd5.year = win_financial_year + 4
        )
        LEFT JOIN contributing_advisers ON contributing_advisers.win_id = export_wins.id
        LEFT JOIN export_wins_hvc_dataset ON export_wins.hvc = CONCAT(export_wins_hvc_dataset.campaign_id, export_wins_hvc_dataset.financial_year)
        ORDER BY export_wins.confirmation_created NULLS FIRST
    '''
