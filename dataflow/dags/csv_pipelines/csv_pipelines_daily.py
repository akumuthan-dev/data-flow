from datetime import datetime

from dataflow.dags import _CSVPipelineDAG
from dataflow.dags.dataset_pipelines import (
    AdvisersDatasetPipeline,
    CompaniesDatasetPipeline,
    ContactsDatasetPipeline,
    EventsDatasetPipeline,
    InteractionsDatasetPipeline,
    InvestmentProjectsDatasetPipeline,
    TeamsDatasetPipeline,
)
from dataflow.dags.derived_report_table_pipelines import (
    ExportWinsDerivedReportTablePipeline,
)


class _DailyCSVPipeline(_CSVPipelineDAG):
    """
    Base DAG to allow subclasses to be picked up by airflow
    """

    schedule_interval = "@daily"
    start_date = datetime(2020, 2, 11)
    timestamp_output = False
    static = True
    catchup = False


class DataHubFDIDailyCSVPipeline(_DailyCSVPipeline):
    """Pipeline meta object for Completed OMIS Order CSV."""

    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        ContactsDatasetPipeline,
        InteractionsDatasetPipeline,
        InvestmentProjectsDatasetPipeline,
        TeamsDatasetPipeline,
    ]

    base_file_name = 'datahub-foreign-direct-investment-daily'
    query = '''
        WITH fdi_report AS (
            WITH companies_last_version AS (
                SELECT *
                FROM dit.data_hub__companies
                    LEFT JOIN (
                        SELECT
                            DISTINCT ON (company_id)
                            company_id AS joined_id,
                            contact_name AS contact_name,
                            is_primary,
                            phone AS contact_phone,
                            email AS contact_email,
                            email_marketing_consent AS contact_accepts_dit_email_marketing
                        FROM dit.data_hub__contacts
                        ORDER BY company_id, is_primary DESC, modified_on DESC
                    ) contacts
                ON data_hub__companies.id = contacts.joined_id
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
                ARRAY_TO_STRING(fdi.competing_countries, '; ') as competing_countries,
                (
                    SELECT STRING_AGG(CONCAT(data_hub__advisers.first_name, ' ', data_hub__advisers.last_name, ' (', teams_dataset.name, ')'), '; ')
                    FROM dit.data_hub__advisers
                    JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
                    WHERE data_hub__advisers.id = ANY(fdi.team_member_ids::uuid[])
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
            FROM dit.data_hub__investment_projects fdi
             LEFT JOIN companies_last_version inv ON fdi.investor_company_id = inv.id
             LEFT JOIN companies_last_version ukc ON fdi.uk_company_id = ukc.id
             LEFT JOIN dit.data_hub__advisers crm ON fdi.client_relationship_manager_id = crm.id
             LEFT JOIN teams_dataset crmt ON crm.team_id = crmt.id
             LEFT JOIN dit.data_hub__advisers paa ON fdi.project_assurance_adviser_id = paa.id
             LEFT JOIN teams_dataset paat ON paa.team_id = paat.id
             LEFT JOIN dit.data_hub__advisers pm ON fdi.project_manager_id = pm.id
             LEFT JOIN teams_dataset pmt ON pm.team_id = pmt.id
             LEFT JOIN dit.data_hub__advisers cre ON fdi.created_by_id = cre.id
             LEFT JOIN teams_dataset cret ON cre.team_id = cret.id
             LEFT JOIN dit.data_hub__advisers mod ON fdi.modified_by_id = mod.id
             LEFT JOIN teams_dataset modt ON mod.team_id = modt.id
             LEFT JOIN dit.data_hub__advisers acm ON inv.one_list_account_owner_id = acm.id
             LEFT JOIN teams_dataset acmt ON acm.team_id = acmt.id
             LEFT JOIN (
                SELECT investment_project_id, max(interaction_date)::text as date_of_latest_interaction
                FROM dit.data_hub__interactions i
                WHERE investment_project_id IS NOT NULL
                GROUP BY investment_project_id
             ) i ON fdi.id = i.investment_project_id
            WHERE (
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
            actual_land_date, estimated_land_date, created_on, modified_on, date_of_latest_interaction,
            name, description,  project_reference,  total_investment, number_new_jobs,
            number_safeguarded_jobs, client_requirements, address_1, address_2, address_postcode, id,
            client_relationship_manager_id, client_relationship_manager_name, client_relationship_manager_team,
            account_manager_name, account_manager_team, fdi_type, investment_type, investor_company_id,
            investor_company_name, investor_company_comp_house_id, investor_company_headquarter_type,
            investor_company_company_tier, investor_company_sector, investor_company_address_1,
            investor_company_address_2, investor_company_uk_region, investor_company_country,
            investor_company_contact_name, investor_company_contact_marked_as_primary_contact,
            investor_company_contact_phone, investor_company_contact_email,
            investor_company_contact_accepts_dit_email_marketing, specific_programme, stage,
            project_assurance_adviser_name, project_assurance_adviser_team,project_manager_name,
            project_manager_team, sector, uk_company_id, uk_company_name, uk_company_comp_house_id,
            uk_company_sector, uk_company_address_1, uk_company_address_2, uk_company_postcode,
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


class DataHubServiceDeliveriesCurrentYearDailyCSVPipeline(_DailyCSVPipeline):
    """Daily updated service deliveries report"""

    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        ContactsDatasetPipeline,
        EventsDatasetPipeline,
        InteractionsDatasetPipeline,
        TeamsDatasetPipeline,
    ]

    base_file_name = 'datahub-service-deliveries-current-calendar-year'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM dit.data_hub__interactions
            WHERE data_hub__interactions.interaction_kind = 'service_delivery'
            AND date_trunc('year', data_hub__interactions.interaction_date) = date_trunc('year', CURRENT_DATE)
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM dit.data_hub__contacts
            JOIN contact_ids ON data_hub__contacts.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, data_hub__contacts.is_primary DESC NULLS LAST
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            data_hub__companies.name AS "Company Name",
            data_hub__companies.company_number AS "Companies HouseID",
            data_hub__companies.id AS "Data Hub Company ID",
            data_hub__companies.cdms_reference_code AS "CDMS Reference Code",
            data_hub__companies.address_postcode AS "Company Postcode",
            data_hub__companies.address_1 AS "Company Address Line 1",
            data_hub__companies.address_2 AS "Company Address Line 2",
            data_hub__companies.address_town AS "Company Address Town",
            data_hub__companies.address_country AS "Company Address Country",
            data_hub__companies.website AS "Company Website",
            data_hub__companies.number_of_employees AS "Number of Employees",
            data_hub__companies.is_number_of_employees_estimated AS "Number of Employees Estimated",
            data_hub__companies.turnover AS "Turnover",
            data_hub__companies.is_turnover_estimated AS "Turnover Estimated",
            data_hub__companies.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            data_hub__advisers.first_name AS "DIT Adviser First Name",
            data_hub__advisers.last_name AS "DIT Adviser Last Name",
            data_hub__advisers.telephone_number AS "DIT Adviser Phone",
            data_hub__advisers.contact_email AS "DIT Adviser Email",
            teams_dataset.name AS "DIT Team",
            data_hub__companies.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            data_hub__events.name AS "Event Name",
            data_hub__events.event_type AS "Event Type",
            to_char(data_hub__events.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            data_hub__events.address_town AS "Event Town",
            data_hub__events.address_country AS "Event Country",
            data_hub__events.uk_region AS "Event UK Region",
            data_hub__events.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link",
            CONCAT(lead_adviser.first_name, ' ', lead_adviser.last_name) as "Lead Adviser Name",
            CONCAT(lead_adviser.contact_email) as "Lead Adviser Email"
        FROM interactions
        JOIN dit.data_hub__companies ON interactions.company_id = data_hub__companies.id
        JOIN dit.data_hub__advisers ON interactions.adviser_ids[1]::uuid = data_hub__advisers.id
        JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
        LEFT JOIN dit.data_hub__events ON interactions.event_id = data_hub__events.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        LEFT JOIN data_hub__advisers lead_adviser ON data_hub__companies.one_list_account_owner_id = lead_adviser.id
        ORDER BY interactions.interaction_date
    '''


class DataHubInteractionsCurrentYearDailyCSVPipeline(_DailyCSVPipeline):
    """Daily updated interactions report"""

    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        ContactsDatasetPipeline,
        EventsDatasetPipeline,
        InteractionsDatasetPipeline,
        TeamsDatasetPipeline,
    ]

    base_file_name = 'datahub-interactions-current-calendar-year'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM dit.data_hub__interactions
            WHERE data_hub__interactions.interaction_kind = 'interaction'
            AND date_trunc('year', data_hub__interactions.interaction_date) = date_trunc('year', CURRENT_DATE)
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM dit.data_hub__contacts
            JOIN contact_ids ON data_hub__contacts.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, data_hub__contacts.is_primary DESC NULLS LAST
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            data_hub__companies.name AS "Company Name",
            data_hub__companies.company_number AS "Companies HouseID",
            data_hub__companies.id AS "Data Hub Company ID",
            data_hub__companies.cdms_reference_code AS "CDMS Reference Code",
            data_hub__companies.address_postcode AS "Company Postcode",
            data_hub__companies.address_1 AS "Company Address Line 1",
            data_hub__companies.address_2 AS "Company Address Line 2",
            data_hub__companies.address_town AS "Company Address Town",
            data_hub__companies.address_country AS "Company Address Country",
            data_hub__companies.website AS "Company Website",
            data_hub__companies.number_of_employees AS "Number of Employees",
            data_hub__companies.is_number_of_employees_estimated AS "Number of Employees Estimated",
            data_hub__companies.turnover AS "Turnover",
            data_hub__companies.is_turnover_estimated AS "Turnover Estimated",
            data_hub__companies.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            data_hub__advisers.first_name AS "DIT Adviser First Name",
            data_hub__advisers.last_name AS "DIT Adviser Last Name",
            data_hub__advisers.telephone_number AS "DIT Adviser Phone",
            data_hub__advisers.contact_email AS "DIT Adviser Email",
            teams_dataset.name AS "DIT Team",
            data_hub__companies.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            data_hub__events.name AS "Event Name",
            data_hub__events.event_type AS "Event Type",
            to_char(data_hub__events.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            data_hub__events.address_town AS "Event Town",
            data_hub__events.address_country AS "Event Country",
            data_hub__events.uk_region AS "Event UK Region",
            data_hub__events.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link"
        FROM interactions
        JOIN dit.data_hub__companies ON interactions.company_id = data_hub__companies.id
        JOIN dit.data_hub__advisers ON interactions.adviser_ids[1]::uuid = data_hub__advisers.id
        JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
        LEFT JOIN dit.data_hub__events ON interactions.event_id = data_hub__events.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        ORDER BY interactions.interaction_date
    '''


class DataHubServiceDeliveriesPreviousYearDailyCSVPipeline(_DailyCSVPipeline):
    """Daily updated service deliveries report for previous calendar year"""

    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        ContactsDatasetPipeline,
        EventsDatasetPipeline,
        InteractionsDatasetPipeline,
        TeamsDatasetPipeline,
    ]

    base_file_name = 'datahub-service-deliveries-previous-calendar-year'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM dit.data_hub__interactions
            WHERE data_hub__interactions.interaction_kind = 'service_delivery'
            AND date_trunc('year', data_hub__interactions.interaction_date) = date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM dit.data_hub__contacts
            JOIN contact_ids ON data_hub__contacts.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, data_hub__contacts.is_primary DESC NULLS LAST
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            data_hub__companies.name AS "Company Name",
            data_hub__companies.company_number AS "Companies HouseID",
            data_hub__companies.id AS "Data Hub Company ID",
            data_hub__companies.cdms_reference_code AS "CDMS Reference Code",
            data_hub__companies.address_postcode AS "Company Postcode",
            data_hub__companies.address_1 AS "Company Address Line 1",
            data_hub__companies.address_2 AS "Company Address Line 2",
            data_hub__companies.address_town AS "Company Address Town",
            data_hub__companies.address_country AS "Company Address Country",
            data_hub__companies.website AS "Company Website",
            data_hub__companies.number_of_employees AS "Number of Employees",
            data_hub__companies.is_number_of_employees_estimated AS "Number of Employees Estimated",
            data_hub__companies.turnover AS "Turnover",
            data_hub__companies.is_turnover_estimated AS "Turnover Estimated",
            data_hub__companies.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            data_hub__advisers.first_name AS "DIT Adviser First Name",
            data_hub__advisers.last_name AS "DIT Adviser Last Name",
            data_hub__advisers.telephone_number AS "DIT Adviser Phone",
            data_hub__advisers.contact_email AS "DIT Adviser Email",
            teams_dataset.name AS "DIT Team",
            data_hub__companies.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            data_hub__events.name AS "Event Name",
            data_hub__events.event_type AS "Event Type",
            to_char(data_hub__events.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            data_hub__events.address_town AS "Event Town",
            data_hub__events.address_country AS "Event Country",
            data_hub__events.uk_region AS "Event UK Region",
            data_hub__events.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link",
            CONCAT(lead_adviser.first_name, ' ', lead_adviser.last_name) as "Lead Adviser Name",
            CONCAT(lead_adviser.contact_email) as "Lead Adviser Email"
        FROM interactions
        JOIN dit.data_hub__companies ON interactions.company_id = data_hub__companies.id
        JOIN dit.data_hub__advisers ON interactions.adviser_ids[1]::uuid = data_hub__advisers.id
        JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
        LEFT JOIN dit.data_hub__events ON interactions.event_id = data_hub__events.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        LEFT JOIN data_hub__advisers lead_adviser ON data_hub__companies.one_list_account_owner_id = lead_adviser.id
        ORDER BY interactions.interaction_date
    '''


class DataHubInteractionsPreviousYearDailyCSVPipeline(_DailyCSVPipeline):
    """Daily updated interactions report for previous calendar year"""

    dependencies = [
        AdvisersDatasetPipeline,
        CompaniesDatasetPipeline,
        ContactsDatasetPipeline,
        EventsDatasetPipeline,
        InteractionsDatasetPipeline,
        TeamsDatasetPipeline,
    ]

    base_file_name = 'datahub-interactions-previous-calendar-year'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM dit.data_hub__interactions
            WHERE data_hub__interactions.interaction_kind = 'interaction'
            AND date_trunc('year', data_hub__interactions.interaction_date) = date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM dit.data_hub__contacts
            JOIN contact_ids ON data_hub__contacts.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, data_hub__contacts.is_primary DESC NULLS LAST
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            data_hub__companies.name AS "Company Name",
            data_hub__companies.company_number AS "Companies HouseID",
            data_hub__companies.id AS "Data Hub Company ID",
            data_hub__companies.cdms_reference_code AS "CDMS Reference Code",
            data_hub__companies.address_postcode AS "Company Postcode",
            data_hub__companies.address_1 AS "Company Address Line 1",
            data_hub__companies.address_2 AS "Company Address Line 2",
            data_hub__companies.address_town AS "Company Address Town",
            data_hub__companies.address_country AS "Company Address Country",
            data_hub__companies.website AS "Company Website",
            data_hub__companies.number_of_employees AS "Number of Employees",
            data_hub__companies.is_number_of_employees_estimated AS "Number of Employees Estimated",
            data_hub__companies.turnover AS "Turnover",
            data_hub__companies.is_turnover_estimated AS "Turnover Estimated",
            data_hub__companies.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            data_hub__advisers.first_name AS "DIT Adviser First Name",
            data_hub__advisers.last_name AS "DIT Adviser Last Name",
            data_hub__advisers.telephone_number AS "DIT Adviser Phone",
            data_hub__advisers.contact_email AS "DIT Adviser Email",
            teams_dataset.name AS "DIT Team",
            data_hub__companies.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            data_hub__events.name AS "Event Name",
            data_hub__events.event_type AS "Event Type",
            to_char(data_hub__events.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            data_hub__events.address_town AS "Event Town",
            data_hub__events.address_country AS "Event Country",
            data_hub__events.uk_region AS "Event UK Region",
            data_hub__events.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link"
        FROM interactions
        JOIN dit.data_hub__companies ON interactions.company_id = data_hub__companies.id
        JOIN dit.data_hub__advisers ON interactions.adviser_ids[1]::uuid = data_hub__advisers.id
        JOIN teams_dataset ON data_hub__advisers.team_id = teams_dataset.id
        LEFT JOIN dit.data_hub__events ON interactions.event_id = data_hub__events.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        ORDER BY interactions.interaction_date    '''


class ExportWinsCurrentFinancialYearDailyCSVPipeline(_DailyCSVPipeline):
    """Daily updated export wins for current financial year"""

    dependencies = [ExportWinsDerivedReportTablePipeline]

    base_file_name = 'export-wins-current-financial-year'
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
        WHERE confirmation_created_financial_year IS NULL OR confirmation_created_financial_year = current_financial_year;
        '''
