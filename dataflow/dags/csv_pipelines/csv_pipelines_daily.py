from datetime import datetime

from dataflow.dags.csv_pipeline import BaseCSVPipeline


class BaseDailyCSVPipeline(BaseCSVPipeline):
    """
    Base DAG to allow subclasses to be picked up by airflow
    """

    schedule_interval = '0 5 * * *'
    start_date = datetime(2020, 2, 11)
    timestamp_output = False
    static = True


class DataHubFDIDailyCSVPipeline(BaseDailyCSVPipeline):
    """Pipeline meta object for Completed OMIS Order CSV."""

    base_file_name = 'fdi_daily'
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
            ARRAY_TO_STRING(fdi.delivery_partners, '; ') AS delivery_partners,
            ARRAY_TO_STRING(fdi.possible_uk_regions, '; ') AS possible_uk_regions,
            ARRAY_TO_STRING(fdi.actual_uk_regions, '; ') AS actual_uk_regions,
            CASE
              WHEN fdi.other_business_activity IS NULL AND fdi.business_activities IS NOT NULL
                THEN ARRAY_TO_STRING(fdi.business_activities, '; ')
              WHEN fdi.other_business_activity IS NOT NULL AND fdi.business_activities IS NULL
                THEN fdi.other_business_activity
              WHEN fdi.other_business_activity IS NOT NULL
                   AND fdi.business_activities IS NOT NULL
                THEN CONCAT(fdi.other_business_activity, ', ', ARRAY_TO_STRING(fdi.business_activities, '; '))
            END AS business_activities,
            fdi.project_arrived_in_triage_on,
            fdi.proposal_deadline,
            CASE WHEN fdi.export_revenue THEN 'yes' ELSE 'no' END AS export_revenue,
            ARRAY_TO_STRING(fdi.strategic_drivers, '; ') as strategic_drivers,
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
        WHERE (
            (fdi.actual_land_date >= '2018-04-01' OR fdi.estimated_land_date >= '2018-04-01')
            AND LOWER(fdi.status) IN ('ongoing', 'won')
        )
        OR (
            (fdi.modified_on BETWEEN (now() - interval '1 year') and :run_date)
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


for pipeline in BaseDailyCSVPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag'] = pipeline().get_dag()
