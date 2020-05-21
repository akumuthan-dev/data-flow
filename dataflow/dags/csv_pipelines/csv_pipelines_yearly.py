from datetime import datetime

from dataflow.dags import _CSVPipelineDAG


class _YearlyCSVPipeline(_CSVPipelineDAG):
    """
    Base DAG to allow subclasses to be picked up by airflow
    """

    schedule_interval = '@yearly'


class ExportWinsYearlyCSVPipeline(_YearlyCSVPipeline):
    """Pipeline meta object for the yearly export wins report."""

    base_file_name = 'export-wins-yearly'
    start_date = datetime(2016, 1, 1)

    query = '''
        SELECT
            "ID",
            "User",
            "Organisation or company name",
            "Data Hub (Companies House) or CDMS reference number",
            "Contact name",
            "Job title",
            "Contact email",
            "HQ location",
            "What kind of business deal was this win?",
            "Summarise the support provided to help achieve this win",
            "Overseas customer",
            "What are the goods or services?",
            "Date business won [MM/YY]",
            "Country",
            "Total expected export value",
            "Total expected non export value",
            "Total expected odi value",
            "Does the expected value relate to",
            "Sector",
            "Prosperity Fund",
            "HVC code (if applicable)",
            "HVO Programme (if applicable)",
            "An HVO specialist was involved",
            "E-exporting programme",
            "type of support 1",
            "type of support 2"
            "type of support 3",
            "associated programme 1",
            "associated programme 2",
            "associated programme 3",
            "associated programme 4",
            "associated programme 5",
            "I confirm that this information is complete and accurate",
            "My line manager has confirmed the decision to record this win",
            "Lead officer name",
            "Lead officer email address",
            "Secondary email address",
            "Line manager",
            "team type",
            "HQ team, Region or Post",
            "Medium-sized and high potential companies",
            "Export experience",
            "Created",
            "Audit",
            "Contributing advisors/team",
            "Customer email sent",
            "Customer email date",
            "Export breakdown 1",
            "Export breakdown 2",
            "Export breakdown 3",
            "Export breakdown 4",
            "Export breakdown 5",
            "Non-export breakdown 1",
            "Non-export breakdown 2",
            "Non-export breakdown 3",
            "Non-export breakdown 4",
            "Non-export breakdown 5",
            "Outward Direct Investment breakdown 1",
            "Outward Direct Investment breakdown 2",
            "Outward Direct Investment breakdown 3",
            "Outward Direct Investment breakdown 4",
            "Outward Direct Investment breakdown 5",
            "Customer response received",
            "Date response received",
            "Your name",
            "Please confirm these details are correct",
            "Other comments or changes to the win details",
            "Securing the win overall?",
            "Gaining access to contacts?",
            "Getting information or improved understanding of the country?",
            "Improving your profile or credibility in the country?",
            "Having confidence to explore or expand in the country?",
            "Developing or nurturing critical relationships?",
            "Overcoming a problem in the country (eg legal, regulatory)?",
            "The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)",
            "Our support was a prerequisite to generate this export value",
            "Our support helped you achieve this win more quickly",
            "Estimated value you would have achieved without our support?",
            "Apart from this win, when did your company last export?",
            "Without this win, your company might have stopped exporting",
            "Apart from this win, you already have specific plans to export in the next 12 months",
            "It enabled you to expand into a new market",
            "It enabled you to increase exports as a proportion of your turnover",
            "It enabled you to maintain or expand in an existing market",
            "Would you be willing to be featured in marketing materials?",
            "How did you first hear about DIT (or its predecessor, UKTI)",
            "Other marketing source"
        FROM (
            WITH export_wins AS (
                SELECT *
                FROM export_wins_wins_dataset
                WHERE export_wins_wins_dataset.customer_email_date IS NOT NULL
                AND date_trunc('year', export_wins_wins_dataset.confirmation_created) =  date_trunc('year', :run_date)
            ), export_breakdowns AS (
                SELECT win_id, year, value
                FROM export_wins_breakdowns_dataset
                WHERE win_id IN (select id from export_wins)
                AND year >= EXTRACT(year FROM CURRENT_DATE)::int
                AND export_wins_breakdowns_dataset.type = 'Export'
            ), non_export_breakdowns AS (
                SELECT win_id, year, value
                FROM export_wins_breakdowns_dataset
                WHERE win_id IN (select id from export_wins)
                AND year >= EXTRACT(year FROM CURRENT_DATE)::int
                AND export_wins_breakdowns_dataset.type = 'Non-export'
            ), odi_breakdowns AS (
                SELECT win_id, year, value
                FROM export_wins_breakdowns_dataset
                WHERE win_id IN (select id from export_wins)
                AND year >= EXTRACT(year FROM CURRENT_DATE)::int
                AND export_wins_breakdowns_dataset.type = 'Outward Direct Investment'
            ), contributing_advisers AS (
                SELECT win_id, STRING_AGG(CONCAT('Name: ', name, ', Team: ', team_type, ' - ', hq_team, ' - ', location), ', ') as advisers
                FROM export_wins_advisers_dataset
                GROUP  BY 1
            )
            SELECT
                export_wins.id AS "ID",
                CONCAT(export_wins.user_name, ' <', export_wins.user_email, '>') AS "User",
                export_wins.user_email AS "User email",
                export_wins.company_name AS "Organisation or company name",
                export_wins.cdms_reference AS "Data Hub (Companies House) or CDMS reference number",
                export_wins.customer_name AS "Contact name",
                export_wins.customer_job_title AS "Job title",
                export_wins.customer_email_address AS "Contact email",
                export_wins.customer_location AS "HQ location",
                export_wins.business_type AS "What kind of business deal was this win?",
                export_wins.description AS "Summarise the support provided to help achieve this win",
                export_wins.name_of_customer AS "Overseas customer",
                export_wins.name_of_export AS "What are the goods or services?",
                to_char(export_wins.date, 'DD/MM/YYYY') AS "Date business won [MM/YY]",
                export_wins.country AS "Country",
                COALESCE(export_wins.total_expected_export_value, 0) AS "Total expected export value",
                COALESCE(export_wins.total_expected_non_export_value, 0) AS "Total expected non export value",
                COALESCE(export_wins.total_expected_odi_value, 0) AS "Total expected odi value",
                export_wins.goods_vs_services AS "Does the expected value relate to",
                export_wins.sector AS "Sector",
                COALESCE(export_wins.is_prosperity_fund_related, 'False') AS "Prosperity Fund",
                export_wins_hvc_dataset.name AS "HVC code (if applicable)",
                export_wins.hvo_programme AS "HVO Programme (if applicable)",
                COALESCE(export_wins.has_hvo_specialist_involvement, 'False') AS "An HVO specialist was involved",
                COALESCE(export_wins.is_e_exported, 'False') AS "E-exporting programme",
                export_wins.type_of_support_1 AS "type of support 1",
                export_wins.type_of_support_2 AS "type of support 2",
                export_wins.type_of_support_3 AS "type of support 3",
                export_wins.associated_programme_1 AS "associated programme 1",
                export_wins.associated_programme_2 AS "associated programme 2",
                export_wins.associated_programme_3 AS "associated programme 3",
                export_wins.associated_programme_4 AS "associated programme 4",
                export_wins.associated_programme_5 AS "associated programme 5",
                export_wins.is_personally_confirmed AS "I confirm that this information is complete and accurate",
                export_wins.is_line_manager_confirmed AS "My line manager has confirmed the decision to record this win",
                export_wins.lead_officer_name AS "Lead officer name",
                export_wins.lead_officer_email_address AS "Lead officer email address",
                export_wins.other_official_email_address AS "Secondary email address",
                export_wins.line_manager_name AS "Line manager",
                export_wins.team_type AS "team type",
                export_wins.hq_team AS "HQ team, Region or Post",
                export_wins.business_potential AS "Medium-sized and high potential companies",
                export_wins.export_experience AS "Export experience",
                to_char(export_wins.created, 'DD/MM/YYYY') AS "Created",
                export_wins.audit AS "Audit",
                contributing_advisers.advisers AS "Contributing advisors/team",
                CASE WHEN export_wins.customer_email_date IS NOT NULL
                THEN 'Yes'
                ELSE 'No'
                END AS "Customer email sent",
                to_char(export_wins.customer_email_date, 'DD/MM/YYYY') AS "Customer email date",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, ': £', COALESCE(ebd1.value, 0)) AS "Export breakdown 1",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, ': £', COALESCE(ebd2.value, 0)) AS "Export breakdown 2",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, ': £', COALESCE(ebd3.value, 0)) "Export breakdown 3",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, ': £', COALESCE(ebd4.value, 0)) AS "Export breakdown 4",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, ': £', COALESCE(ebd5.value, 0)) AS "Export breakdown 5",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, ': £', COALESCE(nebd1.value, 0)) AS "Non-export breakdown 1",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, ': £', COALESCE(nebd2.value, 0)) AS "Non-export breakdown 2",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, ': £', COALESCE(nebd3.value, 0)) AS "Non-export breakdown 3",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, ': £', COALESCE(nebd4.value, 0)) AS "Non-export breakdown 4",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, ': £', COALESCE(nebd5.value, 0)) AS "Non-export breakdown 5",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int, ': £', COALESCE(odibd1.value, 0)) AS "Outward Direct Investment breakdown 1",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 1, ': £', COALESCE(odibd2.value, 0)) AS "Outward Direct Investment breakdown 2",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 2, ': £', COALESCE(odibd3.value, 0)) AS "Outward Direct Investment breakdown 3",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 3, ': £', COALESCE(odibd4.value, 0)) AS "Outward Direct Investment breakdown 4",
                CONCAT(EXTRACT(year FROM CURRENT_DATE)::int + 4, ': £', COALESCE(odibd5.value, 0)) AS "Outward Direct Investment breakdown 5",
                CASE WHEN export_wins.confirmation_created IS NOT NULL
                THEN 'Yes'
                ELSE 'No'
                END AS "Customer response received",
                to_char(export_wins.confirmation_created, 'DD/MM/YYYY') AS "Date response received",
                export_wins.confirmation_name AS "Your name",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_agree_with_win
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_agree_with_win IS NULL
                    THEN 'No'
                END AS "Please confirm these details are correct",
                export_wins.confirmation_comments AS "Other comments or changes to the win details",
                export_wins.confirmation_our_support AS "Securing the win overall?",
                export_wins.confirmation_access_to_contacts AS "Gaining access to contacts?",
                export_wins.confirmation_access_to_information AS "Getting information or improved understanding of the country?",
                export_wins.confirmation_improved_profile AS "Improving your profile or credibility in the country?",
                export_wins.confirmation_gained_confidence AS "Having confidence to explore or expand in the country?",
                export_wins.confirmation_developed_relationships AS "Developing or nurturing critical relationships?",
                export_wins.confirmation_overcame_problem AS "Overcoming a problem in the country (eg legal, regulatory)?",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_involved_state_enterprise
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_involved_state_enterprise IN (FALSE, NULL)
                    THEN 'No'
                END AS "The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_interventions_were_prerequisite
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_interventions_were_prerequisite IN (FALSE, NULL)
                    THEN 'No'
                END AS "Our support was a prerequisite to generate this export value",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_support_improved_speed
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_support_improved_speed IN (FALSE, NULL)
                    THEN 'No'
                END AS "Our support helped you achieve this win more quickly",
                export_wins.confirmation_portion_without_help AS "Estimated value you would have achieved without our support?",
                export_wins.confirmation_last_export AS "Apart from this win, when did your company last export?",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_company_was_at_risk_of_not_exporting
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_company_was_at_risk_of_not_exporting IN (FALSE, NULL)
                    THEN 'No'
                END AS "Without this win, your company might have stopped exporting",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_explicit_export_plans
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_explicit_export_plans IN (FALSE, NULL)
                    THEN 'No'
                END AS "Apart from this win, you already have specific plans to export in the next 12 months",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_new_market
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_new_market IN (FALSE, NULL)
                    THEN 'No'
                END AS "It enabled you to expand into a new market",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_increased_exports_as_percent_of_turnover
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_increased_exports_as_percent_of_turnover IN (FALSE, NULL)
                    THEN 'No'
                END AS "It enabled you to increase exports as a proportion of your turnover",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_existing_market
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_existing_market IN (FALSE, NULL)
                    THEN 'No'
                END AS "It enabled you to maintain or expand in an existing market",
                CASE
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_case_study_willing
                    THEN 'Yes'
                    WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_case_study_willing IN (FALSE, NULL)
                    THEN 'No'
                END AS "Would you be willing to be featured in marketing materials?",
                export_wins.confirmation_marketing_source AS "How did you first hear about DIT (or its predecessor, UKTI)",
                export_wins.confirmation_other_marketing_source AS "Other marketing source"
            FROM export_wins
            -- Export breakdowns
            LEFT JOIN export_breakdowns ebd1 ON (
                export_wins.id = ebd1.win_id
                AND ebd1.year = extract(year FROM CURRENT_DATE)::int
            )
            LEFT JOIN export_breakdowns ebd2 ON (
                export_wins.id = ebd2.win_id
                AND ebd2.year = extract(year FROM CURRENT_DATE)::int + 1
            )
            LEFT JOIN export_breakdowns ebd3 ON (
                export_wins.id = ebd3.win_id
                AND ebd3.year = extract(year FROM CURRENT_DATE)::int + 2
            )
            LEFT JOIN export_breakdowns ebd4 ON (
                export_wins.id = ebd4.win_id
                AND ebd4.year = extract(year FROM CURRENT_DATE)::int + 3
            )
            LEFT JOIN export_breakdowns ebd5 ON (
                export_wins.id = ebd5.win_id
                AND ebd5.year = extract(year FROM CURRENT_DATE)::int + 4
            )
            -- Non export breakdowns
            LEFT JOIN non_export_breakdowns nebd1 ON (
                export_wins.id = nebd1.win_id
                AND nebd1.year = extract(year FROM CURRENT_DATE)::int
            )
            LEFT JOIN non_export_breakdowns nebd2 ON (
                export_wins.id = nebd2.win_id
                AND nebd2.year = extract(year FROM CURRENT_DATE)::int + 1
            )
            LEFT JOIN non_export_breakdowns nebd3 ON (
                export_wins.id = nebd3.win_id
                AND nebd3.year = extract(year FROM CURRENT_DATE)::int + 2
            )
            LEFT JOIN non_export_breakdowns nebd4 ON (
                export_wins.id = nebd4.win_id
                AND nebd4.year = extract(year FROM CURRENT_DATE)::int + 3
            )
            LEFT JOIN non_export_breakdowns nebd5 ON (
                export_wins.id = nebd5.win_id
                AND nebd5.year = extract(year FROM CURRENT_DATE)::int + 4
            )
            -- Outward Direct Investment breakdowns
            LEFT JOIN odi_breakdowns odibd1 ON (
                export_wins.id = odibd1.win_id
                AND odibd1.year = extract(year FROM CURRENT_DATE)::int
            )
            LEFT JOIN odi_breakdowns odibd2 ON (
                export_wins.id = odibd2.win_id
                AND odibd2.year = extract(year FROM CURRENT_DATE)::int + 1
            )
            LEFT JOIN odi_breakdowns odibd3 ON (
                export_wins.id = odibd3.win_id
                AND odibd3.year = extract(year FROM CURRENT_DATE)::int + 2
            )
            LEFT JOIN odi_breakdowns odibd4 ON (
                export_wins.id = odibd4.win_id
                AND odibd4.year = extract(year FROM CURRENT_DATE)::int + 3
            )
            LEFT JOIN odi_breakdowns odibd5 ON (
                export_wins.id = odibd5.win_id
                AND odibd5.year = extract(year FROM CURRENT_DATE)::int + 4
            )
            LEFT JOIN contributing_advisers ON contributing_advisers.win_id = export_wins.id
            LEFT JOIN export_wins_hvc_dataset ON export_wins.hvc = CONCAT(export_wins_hvc_dataset.campaign_id, export_wins_hvc_dataset.financial_year)
            ORDER BY export_wins.confirmation_created NULLS FIRST
        ) a
        WHERE "Please confirm these details are correct" = 'Yes'
    '''
