from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.operators.db_tables import create_aggregate_table


class BaseAggregateDatasetPipeline:
    target_db = config.DATASETS_DB_NAME
    start_date = datetime(2020, 2, 18)
    end_date = None
    schedule_interval = '0 5 * * *'
    catchup = False

    def get_dag(self):
        with DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
            max_active_runs=1,
        ) as dag:
            PythonOperator(
                task_id=f'create-csv-current',
                python_callable=create_aggregate_table,
                provide_context=True,
                op_args=[
                    self.target_db,
                    self.table_name,
                    self.query,
                ],
                dag=dag,
            )
            return dag


class ExportWinsFinancialYearAggPipeline(BaseAggregateDatasetPipeline):
    table_name = 'export_wins_by_financial_year'
    query = '''
        WITH export_wins AS (
            SELECT
                *,
                CASE WHEN EXTRACT('month' FROM date)::int >= 4
                THEN (to_char(date, 'YYYY')::int)
                ELSE (to_char(date + interval '-1' year, 'YYYY')::int)
                END win_financial_year
            FROM export_wins_wins_dataset
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
            CASE WHEN EXTRACT('month' FROM export_wins.confirmation_created)::int >= 4
            THEN (to_char(export_wins.confirmation_created, 'YYYY-04'))
            ELSE (to_char(export_wins.confirmation_created + interval '-1' year, 'YYYY-04'))
            END confirmation_created_financial_year,
            export_wins.win_financial_year,
            export_wins.id win_id,
            export_wins.user_name,
            export_wins.user_email,
            export_wins.company_name,
            export_wins.cdms_reference company_number,
            export_wins.customer_name,
            export_wins.customer_job_title,
            export_wins.customer_email_address,
            export_wins.customer_location,
            export_wins.business_type,
            export_wins.description,
            export_wins.name_of_customer,
            export_wins.name_of_export,
            export_wins.date win_date,
            export_wins.country,
            COALESCE(export_wins.total_expected_export_value, 0) total_expected_export_value,
            COALESCE(export_wins.total_expected_non_export_value, 0) total_expected_non_export_value,
            COALESCE(export_wins.total_expected_odi_value, 0) total_expected_odi_value,
            export_wins.goods_vs_services,
            export_wins.sector,
            export_wins.is_prosperity_fund_related,
            export_wins_hvc_dataset.name hvc_code,
            export_wins.hvo_programme hvo_programme,
            export_wins.has_hvo_specialist_involvement,
            export_wins.is_e_exported,
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
            export_wins.created AS win_created,
            export_wins.audit,
            contributing_advisers.advisers,
            CASE WHEN export_wins.customer_email_date IS NOT NULL
            THEN TRUE
            ELSE FALSE
            END customer_email_sent,
            export_wins.customer_email_date,
            CONCAT(win_financial_year, ': £', COALESCE(ebd1.value, 0)) export_breakdown_1,
            CONCAT(win_financial_year + 1, ': £', COALESCE(ebd2.value, 0)) export_breakdown_2,
            CONCAT(win_financial_year + 2, ': £', COALESCE(ebd3.value, 0)) export_breakdown_3,
            CONCAT(win_financial_year + 3, ': £', COALESCE(ebd4.value, 0)) export_breakdown_4,
            CONCAT(win_financial_year + 4, ': £', COALESCE(ebd5.value, 0)) export_breakdown_5,
            CONCAT(win_financial_year, ': £', COALESCE(nebd1.value, 0)) non_export_breakdown_1,
            CONCAT(win_financial_year + 1, ': £', COALESCE(nebd2.value, 0)) non_export_breakdown_2,
            CONCAT(win_financial_year + 2, ': £', COALESCE(nebd3.value, 0)) non_export_breakdown_3,
            CONCAT(win_financial_year + 3, ': £', COALESCE(nebd4.value, 0)) non_export_breakdown_4,
            CONCAT(win_financial_year + 4, ': £', COALESCE(nebd5.value, 0)) non_export_breakdown_5,
            CONCAT(win_financial_year, ': £', COALESCE(odibd1.value, 0)) odi_breakdown_1,
            CONCAT(win_financial_year + 1, ': £', COALESCE(odibd2.value, 0)) odi_breakdown_2,
            CONCAT(win_financial_year + 2, ': £', COALESCE(odibd3.value, 0)) odi_breakdown_3,
            CONCAT(win_financial_year + 3, ': £', COALESCE(odibd4.value, 0)) odi_breakdown_4,
            CONCAT(win_financial_year + 4, ': £', COALESCE(odibd5.value, 0)) odi_breakdown_5,
            CASE WHEN export_wins.confirmation_created IS NOT NULL
            THEN TRUE
            ELSE FALSE
            END customer_response_received,
            export_wins.confirmation_created date_response_received,
            export_wins.confirmation_name confirmation_name,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_agree_with_win
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_agree_with_win IS NULL
                THEN FALSE
            END confirmation_agree_with_win,
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
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_involved_state_enterprise IS NULL
                THEN FALSE
            END confirmation_involved_state_enterprise,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_interventions_were_prerequisite
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_interventions_were_prerequisite IS NULL
                THEN FALSE
            END confirmation_interventions_were_prerequisite,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_support_improved_speed
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_support_improved_speed IS NULL
                THEN FALSE
            END confirmation_support_improved_speed,
            export_wins.confirmation_portion_without_help,
            export_wins.confirmation_last_export,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_company_was_at_risk_of_not_exporting
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_company_was_at_risk_of_not_exporting IS NULL
                THEN FALSE
            END confirmation_company_was_at_risk_of_not_exporting,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_explicit_export_plans
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_explicit_export_plans IS NULL
                THEN FALSE
            END confirmation_has_explicit_export_plans,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_new_market
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_new_market IS NULL
                THEN FALSE
            END confirmation_has_enabled_expansion_into_new_market,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_increased_exports_as_percent_of_turnover
                THEN FALSE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_increased_exports_as_percent_of_turnover IS NULL
                THEN TRUE
            END confirmation_has_increased_exports_as_percent_of_turnover,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_existing_market
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_has_enabled_expansion_into_existing_market IS NULL
                THEN FALSE
            END confirmation_has_enabled_expansion_into_existing_market,
            CASE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_case_study_willing
                THEN TRUE
                WHEN export_wins.confirmation_created IS NOT NULL AND export_wins.confirmation_case_study_willing IS NULL
                THEN FALSE
            END confirmation_case_study_willing,
            export_wins.confirmation_marketing_source confirmation_marketing_source,
            export_wins.confirmation_other_marketing_source confirmation_other_marketing_source
        FROM export_wins
        -- Export breakdowns
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
        -- Non export breakdowns
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
        -- Outward Direct Investment breakdowns
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
    '''


class DataHubCompanyInteractionServiceDeliveryAggPipeline(BaseAggregateDatasetPipeline):
    table_name = 'company_interaction_and_service_delivery'
    query = '''
        SELECT
            interactions_dataset.interaction_date,
            interactions_dataset.interaction_kind,
            companies_dataset.name company_name,
            companies_dataset.company_number companies_house_id,
            companies_dataset.id datahub_company_id,
            companies_dataset.cdms_reference_code cdms_reference_code,
            companies_dataset.address_1 company_address_1,
            companies_dataset.address_2 company_address_2,
            companies_dataset.address_town company_address_town,
            companies_dataset.address_postcode company_address_postcode,
            companies_dataset.uk_region company_uk_region,
            companies_dataset.website company_website,
            companies_dataset.number_of_employees,
            companies_dataset.is_number_of_employees_estimated,
            companies_dataset.turnover,
            companies_dataset.is_turnover_estimated,
            companies_dataset.sector,
            contacts_dataset.contact_name,
            contacts_dataset.phone contact_phone,
            contacts_dataset.email contact_email,
            contacts_dataset.address_1 contact_address_1,
            contacts_dataset.address_2 contact_address_2,
            contacts_dataset.address_town contact_address_town,
            contacts_dataset.address_postcode contact_address_postcode,
            contacts_dataset.address_country contact_address_country,
            advisers_dataset.first_name adviser_first_name,
            advisers_dataset.last_name adviser_last_name,
            advisers_dataset.telephone_number adviser_telephone_number,
            advisers_dataset.contact_email adviser_email,
            teams_dataset.name team_name,
            interactions_dataset.service_delivery,
            interactions_dataset.interaction_subject,
            interactions_dataset.interaction_notes,
            interactions_dataset.net_company_receipt,
            interactions_dataset.grant_amount_offered,
            interactions_dataset.service_delivery_status,
            events_dataset.name event_name,
            events_dataset.event_type,
            events_dataset.start_date event_start_date,
            events_dataset.address_town event_address_town,
            events_dataset.address_country event_address_country,
            events_dataset.uk_region event_uk_region,
            events_dataset.service_name event_service_name,
            interactions_dataset.created_on,
            interactions_dataset.communication_channel,
            interactions_dataset.interaction_link
        FROM interactions_dataset
        JOIN companies_dataset ON interactions_dataset.company_id = companies_dataset.id
        JOIN advisers_dataset ON interactions_dataset.adviser_ids[1]::uuid = advisers_dataset.id
        JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
        LEFT JOIN events_dataset ON interactions_dataset.event_id = events_dataset.id
        JOIN contacts_dataset ON contacts_dataset.id = (
            select contact_id
            FROM (
                SELECT UNNEST(contact_ids)::uuid AS contact_id
                FROM interactions_dataset i
                WHERE interactions_dataset.id=i.id
            ) nested_contacts
            JOIN contacts_dataset ON contacts_dataset.id = contact_id
            ORDER BY contacts_dataset.is_primary DESC NULLS LAST
            LIMIT 1
        )
    '''


class DataHubCurrentFDIProjectsAggPipeline(BaseAggregateDatasetPipeline):
    table_name = 'current_fdi_projects'
    query = '''
        WITH fdi_report AS (
            WITH companies_last_version AS (
                SELECT *
                FROM companies_dataset
                    LEFT JOIN (
                        SELECT
                            DISTINCT ON (company_id)
                            company_id AS joined_id,
                            contact_name AS contact_name,
                            is_primary,
                            phone AS contact_phone,
                            email AS contact_email,
                            accepts_dit_email_marketing AS contact_accepts_dit_email_marketing
                        FROM contacts_dataset
                        ORDER BY company_id, is_primary DESC, modified_on DESC
                    ) contacts
                ON companies_dataset.id = contacts.joined_id
            )
            SELECT
                fdi.id AS unique_id,
                to_char(fdi.actual_land_date, 'YYYY-MM-DD') AS actual_land_date,
                to_char(fdi.estimated_land_date, 'YYYY-MM-DD') AS estimated_land_date,
                to_char(fdi.created_on, 'YYYY-MM-DD') AS created_on,
                to_char(fdi.modified_on, 'YYYY-MM-DD') AS modified_on,
                SUBSTRING(i.date_of_latest_interaction, 0, 11) AS date_of_latest_interaction,
                fdi.name,
                fdi.description,
                fdi.project_reference,
                fdi.total_investment,
                fdi.number_new_jobs,
                fdi.number_safeguarded_jobs,
                fdi.client_requirements,
                fdi.address_1,
                fdi.address_2,
                fdi.address_postcode,
                fdi.id,
                fdi.client_relationship_manager_id,
                crm.first_name || ' ' || crm.last_name AS client_relationship_manager_name,
                crmt.name AS client_relationship_manager_team,
                acm.first_name || ' ' || acm.last_name AS account_manager_name,
                acmt.name AS account_manager_team,
                fdi.fdi_type,
                fdi.investment_type,
                fdi.investor_company_id,
                inv.name AS investor_company_name,
                inv.company_number AS investor_company_comp_house_id,
                inv.headquarter_type AS investor_company_headquarter_type,
                inv.one_list_tier AS investor_company_company_tier,
                inv.sector AS investor_company_sector,
                inv.address_1 AS investor_company_address_1,
                inv.address_2 AS investor_company_address_2,
                inv.uk_region AS investor_company_uk_region,
                inv.address_country AS investor_company_country,
                inv.contact_name AS investor_company_contact_name,
                inv.is_primary AS investor_company_contact_marked_as_primary_contact,
                inv.contact_phone AS investor_company_contact_phone,
                inv.contact_email AS investor_company_contact_email,
                inv.contact_accepts_dit_email_marketing AS investor_company_contact_accepts_dit_email_marketing,
                inv.one_list_account_owner_id,
                fdi.specific_programme,
                fdi.stage,
                paa.first_name || ' ' || paa.last_name AS project_assurance_adviser_name,
                paat.name AS project_assurance_adviser_team,
                pm.first_name || ' ' || pm.last_name AS project_manager_name,
                pmt.name AS project_manager_team,
                fdi.sector,
                fdi.uk_company_id,
                ukc.name AS uk_company_name,
                ukc.company_number AS uk_company_comp_house_id,
                ukc.sector AS uk_company_sector,
                ukc.address_1 AS uk_company_address_1,
                ukc.address_2 AS uk_company_address_2,
                ukc.address_postcode AS uk_company_postcode,
                ukc.uk_region AS uk_company_uk_region,
                ukc.address_country AS uk_company_country,
                ukc.contact_name AS uk_company_contact_name,
                ukc.is_primary AS uk_company_contact_marked_as_primary_contact,
                ukc.contact_phone AS uk_company_contact_phone,
                ukc.contact_email AS uk_company_contact_email,
                ukc.contact_accepts_dit_email_marketing AS uk_company_contact_accepts_dit_email_marketing,
                fdi.likelihood_to_land,
                fdi.fdi_value,
                cre.first_name || ' ' || cre.last_name AS created_by_name,
                cret.name AS created_by_team,
                mod.first_name || ' ' || mod.last_name AS modified_by_name,
                modt.name as modified_by_team,
                fdi.status,
                fdi.anonymous_description,
                fdi.associated_non_fdi_r_and_d_project_id,
                array_to_string(fdi.competing_countries, '; ') as competing_countries,
                (
                    SELECT STRING_AGG(CONCAT(advisers_dataset.first_name, ' ', advisers_dataset.last_name, ' (', teams_dataset.name, ')'), '; ')
                    FROM advisers_dataset
                    JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
                    WHERE advisers_dataset.id = ANY(fdi.team_member_ids::uuid[])
                ) AS team_members,
                fdi.investor_type,
                fdi.level_of_involvement,
                fdi.foreign_equity_investment,
                fdi.government_assistance,
                fdi.r_and_d_budget,
                fdi.non_fdi_r_and_d_budget,
                fdi.new_tech_to_uk,
                fdi.average_salary,
                fdi.referral_source_activity,
                fdi.referral_source_activity_website,
                fdi.referral_source_activity_marketing,
                fdi.delivery_partners,
                fdi.possible_uk_regions,
                fdi.actual_uk_regions,
                CASE
                  WHEN fdi.other_business_activity IS NULL AND fdi.business_activities IS NOT NULL
                    THEN ARRAY_TO_STRING(fdi.business_activities, '; ')
                  WHEN fdi.other_business_activity IS NOT NULL AND fdi.business_activities IS NULL
                    THEN fdi.other_business_activity
                  WHEN fdi.other_business_activity IS NOT NULL AND fdi.business_activities IS NOT NULL
                    THEN CONCAT(fdi.other_business_activity, '; ', ARRAY_TO_STRING(fdi.business_activities, '; '))
                  END AS business_activities,
                fdi.project_arrived_in_triage_on,
                fdi.proposal_deadline,
                CASE WHEN fdi.export_revenue THEN TRUE ELSE FALSE END AS export_revenue,
                fdi.strategic_drivers,
                fdi.gross_value_added,
                fdi.gva_multiplier
            FROM investment_projects_dataset fdi
             LEFT JOIN companies_last_version inv ON fdi.investor_company_id = inv.id
             LEFT JOIN companies_last_version ukc ON fdi.uk_company_id = ukc.id
             LEFT JOIN advisers_dataset crm ON fdi.client_relationship_manager_id = crm.id
             LEFT JOIN teams_dataset crmt ON crm.team_id = crmt.id
             LEFT JOIN advisers_dataset paa ON fdi.project_assurance_adviser_id = paa.id
             LEFT JOIN teams_dataset paat ON paa.team_id = paat.id
             LEFT JOIN advisers_dataset pm ON fdi.project_manager_id = pm.id
             LEFT JOIN teams_dataset pmt ON pm.team_id = pmt.id
             LEFT JOIN advisers_dataset cre ON fdi.created_by_id = cre.id
             LEFT JOIN teams_dataset cret ON cre.team_id = cret.id
             LEFT JOIN advisers_dataset mod ON fdi.modified_by_id = mod.id
             LEFT JOIN teams_dataset modt ON mod.team_id = modt.id
             LEFT JOIN advisers_dataset acm ON inv.one_list_account_owner_id = acm.id
             LEFT JOIN teams_dataset acmt ON acm.team_id = acmt.id
             LEFT JOIN (
                SELECT investment_project_id, max(interaction_date)::text as date_of_latest_interaction
                FROM interactions_dataset i
                WHERE investment_project_id IS NOT NULL
                GROUP BY investment_project_id
             ) i ON fdi.id = i.investment_project_id
            WHERE
            (
                (fdi.actual_land_date >= '2018-04-01' OR fdi.estimated_land_date >= '2018-04-01')
                AND LOWER(fdi.status) IN ('ongoing', 'won')
            )
            OR (
                (fdi.modified_on BETWEEN (now() - interval '1 year') and now())
                AND LOWER(fdi.status) NOT IN ('ongoing', 'won')
            )
            ORDER BY fdi.actual_land_date, fdi.estimated_land_date ASC
        )
        SELECT DISTINCT ON (unique_id)
            actual_land_date, estimated_land_date, created_on, modified_on, date_of_latest_interaction, name,
            description,  project_reference,  total_investment, number_new_jobs,
            number_safeguarded_jobs, client_requirements, address_1, address_2, address_postcode, id,
            client_relationship_manager_id, client_relationship_manager_name, client_relationship_manager_team,
            account_manager_name, account_manager_team, fdi_type, investment_type, investor_company_id,
            investor_company_name, investor_company_comp_house_id, investor_company_headquarter_type,
            investor_company_company_tier, investor_company_sector, investor_company_address_1, investor_company_address_2,
            investor_company_uk_region, investor_company_country, investor_company_contact_name,
            investor_company_contact_marked_as_primary_contact, investor_company_contact_phone,
            investor_company_contact_email, investor_company_contact_accepts_dit_email_marketing,
            specific_programme, stage, project_assurance_adviser_name, project_assurance_adviser_team,
            project_manager_name, project_manager_team, sector, uk_company_id, uk_company_name,
            uk_company_comp_house_id, uk_company_sector, uk_company_address_1, uk_company_address_2, uk_company_postcode,
            uk_company_uk_region, uk_company_country, uk_company_contact_name, uk_company_contact_marked_as_primary_contact,
            uk_company_contact_phone,  uk_company_contact_email, uk_company_contact_accepts_dit_email_marketing,
            likelihood_to_land, fdi_value, created_by_name, created_by_team, modified_by_name, modified_by_team,
            status, anonymous_description, associated_non_fdi_r_and_d_project_id, competing_countries,
            team_members, investor_type, level_of_involvement, foreign_equity_investment, government_assistance,
            r_and_d_budget, non_fdi_r_and_d_budget, new_tech_to_uk, average_salary, referral_source_activity,
            referral_source_activity_website, referral_source_activity_marketing, delivery_partners,
            possible_uk_regions, actual_uk_regions, business_activities, project_arrived_in_triage_on,
            proposal_deadline, export_revenue, strategic_drivers, gross_value_added, gva_multiplier
        FROM fdi_report f
    '''


for pipeline in BaseAggregateDatasetPipeline.__subclasses__():
    globals()[pipeline.__name__ + "__dag"] = pipeline().get_dag()
