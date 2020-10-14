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
            id AS "ID",
            user_name AS "User",
            company_name AS "Organisation or company name",
            cdms_reference AS "Data Hub (Companies House) or CDMS reference number",
            customer_name AS "Contact name",
            customer_job_title AS "Job title",
            customer_email_address AS "Contact email",
            customer_location AS "HQ location",
            business_type AS "What kind of business deal was this win?",
            description AS "Summarise the support provided to help achieve this win",
            name_of_customer AS "Overseas customer",
            name_of_export AS "What are the goods or services?",
            date_business_won AS "Date business won [MM/YY]",
            country AS "Country",
            total_expected_export_value AS "Total expected export value",
            total_expected_non_export_value AS "Total expected non export value",
            total_expected_odi_value AS "Total expected odi value",
            goods_vs_services AS "Does the expected value relate to",
            sector AS "Sector",
            prosperity_fund_related AS "Prosperity Fund",
            hvc_code AS "HVC code (if applicable)",
            hvo_programme AS "HVO Programme (if applicable)",
            has_hvo_specialist_involvement AS "An HVO specialist was involved",
            is_e_exported AS "E-exporting programme",
            type_of_support_1 AS "type of support 1",
            type_of_support_2 AS "type of support 2",
            type_of_support_3 AS "type of support 3",
            associated_programme_1 AS "associated programme 1",
            associated_programme_2 AS "associated programme 2",
            associated_programme_3 AS "associated programme 3",
            associated_programme_4 AS "associated programme 4",
            associated_programme_5 AS "associated programme 5",
            is_personally_confirmed AS "I confirm that this information is complete and accurate",
            is_line_manager_confirmed AS "My line manager has confirmed the decision to record this win",
            lead_officer_name AS "Lead officer name",
            lead_officer_email_address AS "Lead officer email address",
            other_official_email_address AS "Secondary email address",
            line_manager_name AS "Line manager",
            team_type AS "team type",
            hq_team AS "HQ team, Region or Post",
            business_potential AS "Medium-sized and high potential companies",
            export_experience AS "Export experience",
            created AS "Created",
            audit AS "Audit",
            advisers AS "Contributing advisors/team",
            customer_email_sent AS "Customer email sent",
            customer_email_date AS "Customer email date",
            export_breakdown_1 AS "Export breakdown 1",
            export_breakdown_2 AS "Export breakdown 2",
            export_breakdown_3 AS "Export breakdown 3",
            export_breakdown_4 AS "Export breakdown 4",
            export_breakdown_5 AS "Export breakdown 5",
            non_export_breakdown_1 AS "Non-export breakdown 1",
            non_export_breakdown_2 AS "Non-export breakdown 2",
            non_export_breakdown_3 AS "Non-export breakdown 3",
            non_export_breakdown_4 AS "Non-export breakdown 4",
            non_export_breakdown_5 AS "Non-export breakdown 5",
            odi_breakdown_1 AS "Outward Direct Investment breakdown 1",
            odi_breakdown_2 AS "Outward Direct Investment breakdown 2",
            odi_breakdown_3 AS "Outward Direct Investment breakdown 3",
            odi_breakdown_4 AS "Outward Direct Investment breakdown 4",
            odi_breakdown_5 AS "Outward Direct Investment breakdown 5",
            customer_response_received AS "Customer response received",
            date_response_received AS "Date response received",
            confirmation_name AS "Your name",
            agree_with_win AS "Please confirm these details are correct",
            confirmation_comments AS "Other comments or changes to the win details",
            confirmation_our_support AS "Securing the win overall?",
            confirmation_access_to_contacts AS "Gaining access to contacts?",
            confirmation_access_to_information AS "Getting information or improved understanding of the country?",
            confirmation_improved_profile AS "Improving your profile or credibility in the country?",
            confirmation_gained_confidence AS "Having confidence to explore or expand in the country?",
            confirmation_developed_relationships AS "Developing or nurturing critical relationships?",
            confirmation_overcame_problem AS "Overcoming a problem in the country (eg legal, regulatory)?",
            confirmation_involved_state_enterprise AS "The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)",
            confirmation_interventions_were_prerequisite AS "Our support was a prerequisite to generate this export value",
            confirmation_support_improved_speed AS "Our support helped you achieve this win more quickly",
            confirmation_portion_without_help AS "Estimated value you would have achieved without our support?",
            confirmation_last_export AS "Apart from this win, when did your company last export?",
            confirmation_company_was_at_risk_of_not_exporting AS "Without this win, your company might have stopped exporting",
            confirmation_has_explicit_export_plans AS "Apart from this win, you already have specific plans to export in the next 12 months",
            confirmation_has_enabled_expansion_into_new_market AS "It enabled you to expand into a new market",
            confirmation_has_increased_exports_as_percent_of_turnover AS "It enabled you to increase exports as a proportion of your turnover",
            confirmation_has_enabled_expansion_into_existing_market AS "It enabled you to maintain or expand in an existing market",
            confirmation_case_study_willing AS "Would you be willing to be featured in marketing materials?",
            confirmation_marketing_source AS "How did you first hear about DIT (or its predecessor, UKTI)",
            confirmation_other_marketing_source AS "Other marketing source"
        FROM dit.export_wins__wins__derived
        WHERE date_trunc('year', confirmation_created_date) = date_trunc('year', :run_date)
        AND agree_with_win = 'Yes';
    '''


class ExportWinsByFinancialYearCSVPipeline(_YearlyCSVPipeline):
    """Pipeline meta object for export wins by financial year."""

    base_file_name = 'export-wins-by-financial-year'
    start_date = datetime(2016, 4, 1)
    schedule_interval = '0 5 1 4 *'

    query = '''
        SELECT
            id AS "ID",
            user_name AS "User",
            company_name AS "Organisation or company name",
            cdms_reference AS "Data Hub (Companies House) or CDMS reference number",
            customer_name AS "Contact name",
            customer_job_title AS "Job title",
            customer_email_address AS "Contact email",
            customer_location AS "HQ location",
            business_type AS "What kind of business deal was this win?",
            description AS "Summarise the support provided to help achieve this win",
            name_of_customer AS "Overseas customer",
            name_of_export AS "What are the goods or services?",
            date_business_won AS "Date business won [MM/YY]",
            country AS "Country",
            total_expected_export_value AS "Total expected export value",
            total_expected_non_export_value AS "Total expected non export value",
            total_expected_odi_value AS "Total expected odi value",
            goods_vs_services AS "Does the expected value relate to",
            sector AS "Sector",
            prosperity_fund_related AS "Prosperity Fund",
            hvc_code AS "HVC code (if applicable)",
            hvo_programme AS "HVO Programme (if applicable)",
            has_hvo_specialist_involvement AS "An HVO specialist was involved",
            is_e_exported AS "E-exporting programme",
            type_of_support_1 AS "type of support 1",
            type_of_support_2 AS "type of support 2",
            type_of_support_3 AS "type of support 3",
            associated_programme_1 AS "associated programme 1",
            associated_programme_2 AS "associated programme 2",
            associated_programme_3 AS "associated programme 3",
            associated_programme_4 AS "associated programme 4",
            associated_programme_5 AS "associated programme 5",
            is_personally_confirmed AS "I confirm that this information is complete and accurate",
            is_line_manager_confirmed AS "My line manager has confirmed the decision to record this win",
            lead_officer_name AS "Lead officer name",
            lead_officer_email_address AS "Lead officer email address",
            other_official_email_address AS "Secondary email address",
            line_manager_name AS "Line manager",
            team_type AS "team type",
            hq_team AS "HQ team, Region or Post",
            business_potential AS "Medium-sized and high potential companies",
            export_experience AS "Export experience",
            created AS "Created",
            audit AS "Audit",
            advisers AS "Contributing advisors/team",
            customer_email_sent AS "Customer email sent",
            customer_email_date AS "Customer email date",
            export_breakdown_1 AS "Export breakdown 1",
            export_breakdown_2 AS "Export breakdown 2",
            export_breakdown_3 AS "Export breakdown 3",
            export_breakdown_4 AS "Export breakdown 4",
            export_breakdown_5 AS "Export breakdown 5",
            non_export_breakdown_1 AS "Non-export breakdown 1",
            non_export_breakdown_2 AS "Non-export breakdown 2",
            non_export_breakdown_3 AS "Non-export breakdown 3",
            non_export_breakdown_4 AS "Non-export breakdown 4",
            non_export_breakdown_5 AS "Non-export breakdown 5",
            odi_breakdown_1 AS "Outward Direct Investment breakdown 1",
            odi_breakdown_2 AS "Outward Direct Investment breakdown 2",
            odi_breakdown_3 AS "Outward Direct Investment breakdown 3",
            odi_breakdown_4 AS "Outward Direct Investment breakdown 4",
            odi_breakdown_5 AS "Outward Direct Investment breakdown 5",
            customer_response_received AS "Customer response received",
            date_response_received AS "Date response received",
            confirmation_name AS "Your name",
            agree_with_win AS "Please confirm these details are correct",
            confirmation_comments AS "Other comments or changes to the win details",
            confirmation_our_support AS "Securing the win overall?",
            confirmation_access_to_contacts AS "Gaining access to contacts?",
            confirmation_access_to_information AS "Getting information or improved understanding of the country?",
            confirmation_improved_profile AS "Improving your profile or credibility in the country?",
            confirmation_gained_confidence AS "Having confidence to explore or expand in the country?",
            confirmation_developed_relationships AS "Developing or nurturing critical relationships?",
            confirmation_overcame_problem AS "Overcoming a problem in the country (eg legal, regulatory)?",
            confirmation_involved_state_enterprise AS "The win involved a foreign government or state-owned enterprise (eg as an intermediary or facilitator)",
            confirmation_interventions_were_prerequisite AS "Our support was a prerequisite to generate this export value",
            confirmation_support_improved_speed AS "Our support helped you achieve this win more quickly",
            confirmation_portion_without_help AS "Estimated value you would have achieved without our support?",
            confirmation_last_export AS "Apart from this win, when did your company last export?",
            confirmation_company_was_at_risk_of_not_exporting AS "Without this win, your company might have stopped exporting",
            confirmation_has_explicit_export_plans AS "Apart from this win, you already have specific plans to export in the next 12 months",
            confirmation_has_enabled_expansion_into_new_market AS "It enabled you to expand into a new market",
            confirmation_has_increased_exports_as_percent_of_turnover AS "It enabled you to increase exports as a proportion of your turnover",
            confirmation_has_enabled_expansion_into_existing_market AS "It enabled you to maintain or expand in an existing market",
            confirmation_case_study_willing AS "Would you be willing to be featured in marketing materials?",
            confirmation_marketing_source AS "How did you first hear about DIT (or its predecessor, UKTI)",
            confirmation_other_marketing_source AS "Other marketing source"
        FROM dit.export_wins__wins__derived
        WHERE confirmation_created_date >= date_trunc('day', :run_date)
        AND confirmation_created_date < date_trunc('day', :run_date) + interval '1 year'
        AND agree_with_win = 'Yes';
    '''
