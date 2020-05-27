from datetime import datetime

from dataflow.dags import _CSVPipelineDAG


class _MonthlyCSVPipeline(_CSVPipelineDAG):
    """
    Base DAG to allow subclasses to be picked up by airflow
    """

    schedule_interval = '0 5 1 * *'
    start_date = datetime(2019, 10, 1)


class DataHubOMISCompletedOrdersCSVPipeline(_MonthlyCSVPipeline):
    """Pipeline meta object for Completed OMIS Order CSV."""

    base_file_name = 'datahub-omis-completed-orders'
    query = '''
        SELECT
            omis.omis_order_reference AS "OMIS Order Reference",
            companies.name AS "Company name",
            teams.name AS "DIT Team",
            ROUND(omis.subtotal::numeric/100::numeric,2) AS "Net price",
            omis.uk_region AS "UK Region",
            omis.market AS "Market",
            omis.sector AS "Sector",
            omis.services AS "Services",
            to_char(omis.delivery_date, 'DD/MM/YYYY') AS "Delivery date",
            to_char(omis.payment_received_date, 'DD/MM/YYYY') AS "Payment received date",
            to_char(omis.completion_date, 'DD/MM/YYYY') AS "Completion Date",
            to_char(omis.created_date, 'DD/MM/YYYY') AS "Created date",
            to_char(omis.cancelled_date, 'DD/MM/YYYY') AS "Cancelled date",
            omis.cancellation_reason AS "Cancellation reason",
            to_char(omis.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM datahub.omis
        LEFT JOIN datahub.companies ON omis.company_id=companies.id
        LEFT JOIN datahub.teams ON omis.dit_team_id=teams.id
        WHERE omis.order_status = 'complete'
        AND date_trunc('month', omis.completion_date) = date_trunc('month', :run_date)
        ORDER BY omis.completion_date
        '''


class DataHubOMISCancelledOrdersCSVPipeline(_MonthlyCSVPipeline):
    """Pipeline meta object for Cancelled OMIS Order CSV."""

    base_file_name = 'datahub-omis-cancelled-orders'
    query = '''
        WITH omis AS (
            SELECT
                CASE WHEN EXTRACT('month' FROM cancelled_date)::int >= 4
                    THEN (to_char(omis.cancelled_date, 'YYYY')::int)
                    ELSE (to_char(omis.cancelled_date + interval '-1' year, 'YYYY')::int)
                END as cancelled_date_financial_year,
                CASE WHEN EXTRACT('month' FROM CURRENT_DATE)::int >= 4
                    THEN (to_char(CURRENT_DATE, 'YYYY')::int)
                    ELSE (to_char(CURRENT_DATE + interval '-1' year, 'YYYY')::int)
                END as current_financial_year,
                *
            FROM datahub.omis
        )
        SELECT
            omis.omis_order_reference AS "OMIS Order Reference",
            companies.name AS "Company Name",
            ROUND(omis.subtotal::numeric/100::numeric,2) AS "Net price",
            teams.name AS "DIT Team",
            omis.market AS "Market",
            to_char(omis.created_date, 'DD/MM/YYYY') AS "Created Date",
            to_char(omis.cancelled_date, 'DD/MM/YYYY') AS "Cancelled Date",
            omis.cancellation_reason AS "Cancellation reason",
            to_char(omis.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM omis
        LEFT JOIN datahub.companies ON omis.company_id=companies.id
        LEFT JOIN datahub.teams ON omis.dit_team_id=teams.id
        WHERE omis.order_status = 'cancelled'
        AND omis.cancelled_date_financial_year = omis.current_financial_year
        ORDER BY omis.cancelled_date
    '''


class DataHubOMISAllOrdersCSVPipeline(_MonthlyCSVPipeline):
    """View pipeline for all OMIS orders created up to the end
     of the last calendar month"""

    base_file_name = 'datahub-omis-all-orders'
    start_date = datetime(2019, 12, 1)
    query = '''
        SELECT
            omis.omis_order_reference AS "Order ID",
            omis.order_status AS "Order status",
            companies.name AS "Company",
            teams.name AS "Creator team",
            omis.uk_region AS "UK region",
            omis.market AS "Primary market",
            omis.sector AS "Sector",
            companies.sector AS "Company sector",
            omis.net_price AS "Net price",
            omis.services AS "Services",
            TO_CHAR(omis.created_date, 'YYYY-MM-DD')::DATE AS "Order created",
            TO_CHAR(omis.quote_created_on, 'YYYY-MM-DD')::DATE AS "Quote created",
            TO_CHAR(omis.quote_accepted_on, 'YYYY-MM-DD')::DATE AS "Quote accepted",
            TO_CHAR(omis.delivery_date, 'YYYY-MM-DD')::DATE AS "Planned delivery date",
            omis.vat_cost AS "VAT",
            TO_CHAR(omis.payment_received_date, 'YYYY-MM-DD')::DATE AS "Payment received date",
            TO_CHAR(omis.completion_date, 'YYYY-MM-DD')::DATE AS "Completion date",
            TO_CHAR(omis.cancelled_date, 'YYYY-MM-DD')::DATE AS "Cancellation date",
            omis.refund_created AS "Refund date",
            omis.refund_total_amount AS "Refund amount"
        FROM datahub.omis
        JOIN datahub.companies ON omis.company_id = companies.id
        JOIN datahub.teams on omis.dit_team_id = teams.id
        WHERE omis.created_date < date_trunc('month', :run_date)  + interval '1 month'
    '''


class DataHubOMISClientSurveyStaticCSVPipeline(_MonthlyCSVPipeline):
    """Pipeline meta object for monthly OMIS Client Survey report."""

    base_file_name = 'datahub-omis-client-survey'
    static = True
    query = '''
        SELECT
            companies.name AS "Company Name",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone Number",
            contacts.email AS "Contact Email",
            companies.address_1 AS "Company Trading Address Line 1",
            companies.address_2 AS "Company Trading Address Line 2",
            companies.address_town AS "Company Trading Address Town",
            companies.address_county AS "Company Trading Address County",
            companies.address_country AS "Company Trading Address Country",
            companies.address_postcode AS "Company Trading Address Postcode",
            companies.registered_address_1 AS "Company Registered Address Line 1",
            companies.registered_address_2 AS "Company Registered Address Line 2",
            companies.registered_address_town AS "Company Registered Address Town",
            companies.registered_address_county AS "Company Registered Address County",
            companies.registered_address_country AS "Company Registered Address Country",
            companies.registered_address_postcode AS "Company Registered Address Postcode",
            to_char(omis.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM datahub.omis
        JOIN datahub.companies ON omis.company_id=companies.id
        JOIN datahub.contacts ON omis.contact_id=contacts.id
        WHERE omis.order_status = 'complete'
        AND date_trunc('month', omis.completion_date) = date_trunc('month', :run_date)
        ORDER BY omis.completion_date
    '''


class DataHubServiceDeliveryInteractionsCSVPipeline(_MonthlyCSVPipeline):
    """Pipeline meta object for the data hub service deliveries and interactions report."""

    base_file_name = 'datahub-service-deliveries-and-interactions'
    start_date = datetime(2019, 11, 15)
    schedule_interval = '0 5 15 * *'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM datahub.interactions
            WHERE interactions.interaction_kind = 'service_delivery'
            AND date_trunc('month', interactions.interaction_date) = date_trunc('month', :run_date)
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM datahub.contacts
            JOIN contact_ids ON contacts.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, contacts.is_primary DESC NULLS LAST
        ),
        adviser_ids AS (
            SELECT id AS interaction_id, UNNEST(adviser_ids)::uuid AS adviser_id
            FROM datahub.interactions
        ),
        team_names AS (
            SELECT adviser_ids.interaction_id as iid, STRING_AGG(teams.name, '; ') AS names
            FROM datahub.advisers
            JOIN adviser_ids ON advisers.id = adviser_ids.adviser_id
            JOIN datahub.teams ON advisers.team_id = teams.id
            GROUP BY 1
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            companies.name AS "Company Name",
            companies.company_number AS "Companies HouseID",
            companies.id AS "Data Hub Company ID",
            companies.cdms_reference_code AS "CDMS Reference Code",
            companies.address_postcode AS "Company Postcode",
            companies.address_1 AS "Company Address Line 1",
            companies.address_2 AS "Company Address Line 2",
            companies.address_town AS "Company Address Town",
            companies.address_country AS "Company Address Country",
            companies.website AS "Company Website",
            companies.number_of_employees AS "Number of Employees",
            companies.is_number_of_employees_estimated AS "Number of Employees Estimated",
            companies.turnover AS "Turnover",
            companies.is_turnover_estimated AS "Turnover Estimated",
            companies.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            advisers.first_name AS "DIT Adviser First Name",
            advisers.last_name AS "DIT Adviser Last Name",
            advisers.telephone_number AS "DIT Adviser Phone",
            advisers.contact_email AS "DIT Adviser Email",
            team_names.names AS "DIT Team",
            companies.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            events.name AS "Event Name",
            events.event_type AS "Event Type",
            to_char(events.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            events.address_town AS "Event Town",
            events.address_country AS "Event Country",
            events.uk_region AS "Event UK Region",
            events.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link"
        FROM interactions
        JOIN datahub.companies ON interactions.company_id = companies.id
        JOIN datahub.advisers ON interactions.adviser_ids[1]::uuid = advisers.id
        LEFT JOIN datahub.events ON interactions.event_id = events.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        LEFT JOIN team_names ON team_names.iid = interactions.id
        ORDER BY interactions.interaction_date
    '''


class DataHubExportClientSurveyStaticCSVPipeline(_MonthlyCSVPipeline):
    """Pipeline meta object for the data hub export client survey report."""

    base_file_name = 'datahub-export-client-survey'
    start_date = datetime(2019, 11, 15)
    schedule_interval = '0 5 15 * *'
    static = True
    query = '''
        WITH service_deliveries AS (
            SELECT *
            FROM datahub.interactions
            WHERE interactions.interaction_kind = 'service_delivery'
            AND date_trunc('month', interactions.interaction_date) = date_trunc('month', :run_date)
        ),
        contact_ids AS (
            SELECT id AS service_delivery_id, UNNEST(contact_ids)::uuid AS contact_id
                FROM service_deliveries
        ),
        contacts AS (
                SELECT DISTINCT ON (contact_ids.service_delivery_id) *
                FROM datahub.contacts
                JOIN contact_ids ON contacts.id = contact_ids.contact_id
                ORDER BY contact_ids.service_delivery_id, contacts.is_primary DESC NULLS LAST
        ),
        adviser_ids AS (
            SELECT id AS service_delivery_id, UNNEST(adviser_ids)::uuid AS adviser_id
            FROM service_deliveries
        ),
        team_names AS (
            SELECT adviser_ids.service_delivery_id as sid, STRING_AGG(teams.name, '; ') AS names
            FROM datahub.advisers
            JOIN adviser_ids ON advisers.id = adviser_ids.adviser_id
            JOIN datahub.teams ON advisers.team_id = teams.id
            GROUP BY 1
        ),
        team_roles AS (
            SELECT adviser_ids.service_delivery_id as sid, STRING_AGG(teams.role, '; ') AS roles
            FROM datahub.advisers
            JOIN adviser_ids ON advisers.id = adviser_ids.adviser_id
            JOIN datahub.teams ON advisers.team_id = teams.id
            GROUP BY 1
        )
        SELECT
            to_char(service_deliveries.interaction_date, 'DD/MM/YYYY') AS "Service Delivery Interaction",
            companies.name AS "Company Name",
            companies.company_number as "Companies House ID",
            companies.id AS "Data Hub Company ID",
            companies.cdms_reference_code AS "CDMS Reference Code",
            companies.address_postcode AS "Company Postcode",
            companies.company_number AS "Companies HouseID",
            companies.address_1 AS "Company Address Line 1",
            companies.address_2 AS "Company Address Line 2",
            companies.address_town AS "Company Address Town",
            companies.address_country AS "Company Address Country",
            companies.website AS "Company Website",
            companies.number_of_employees AS "Number of Employees",
            companies.is_number_of_employees_estimated AS "Number of Employees Estimated",
            companies.turnover AS "Turnover",
            companies.is_turnover_estimated AS "Turnover Estimated",
            companies.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            team_names.names AS "DIT Team",
            companies.uk_region AS "Company UK Region",
            service_deliveries.service_delivery AS "Service Delivery",
            service_deliveries.interaction_subject AS "Subject",
            service_deliveries.interaction_notes AS "Notes",
            service_deliveries.net_company_receipt AS "Net Company Receipt",
            service_deliveries.grant_amount_offered AS "Grant Amount Offered",
            service_deliveries.service_delivery_status AS "Service Delivery Status",
            events.name AS "Event Name",
            events.event_type AS "Event Type",
            to_char(events.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            events.address_town AS "Event Town",
            events.address_country AS "Event Country",
            events.uk_region AS "Event UK Region",
            events.service_name AS "Event Service Name",
            team_roles.roles AS "Team Role",
            to_char(service_deliveries.created_on, 'DD/MM/YYYY') AS "Created On Date"
        FROM service_deliveries
        JOIN datahub.companies ON service_deliveries.company_id = companies.id
        LEFT JOIN datahub.events ON service_deliveries.event_id = events.id
        LEFT JOIN contacts ON contacts.service_delivery_id = service_deliveries.id
        LEFT JOIN team_names ON team_names.sid = service_deliveries.id
        LEFT JOIN team_roles ON team_roles.sid = service_deliveries.id
        ORDER BY service_deliveries.interaction_date
    '''


class DataHubFDIMonthlyStaticCSVPipeline(_MonthlyCSVPipeline):
    """Static monthly view of the FDI (investment projects) report"""

    base_file_name = 'datahub-foreign-direct-investment-monthly'
    start_date = datetime(2020, 1, 1)
    static = True
    query = '''
        WITH fdi_report AS (
            WITH companies_last_version AS (
                SELECT *
                FROM datahub.companies
                    LEFT JOIN (
                        SELECT
                            DISTINCT ON (company_id)
                            company_id AS joined_id,
                            contact_name AS contact_name,
                            is_primary,
                            phone AS contact_phone,
                            email AS contact_email,
                            accepts_dit_email_marketing AS contact_accepts_dit_email_marketing
                        FROM datahub.contacts
                        ORDER BY company_id, is_primary DESC, modified_on DESC
                    ) contacts
                ON companies.id = contacts.joined_id
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
                    SELECT STRING_AGG(CONCAT(advisers.first_name, ' ', advisers.last_name, ' (', teams.name, ')'), '; ')
                    FROM datahub.advisers
                    JOIN datahub.teams ON advisers.team_id = teams.id
                    WHERE advisers.id = ANY(fdi.team_member_ids::uuid[])
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
                  WHEN fdi.other_business_activity IN (NULL, '') AND fdi.business_activities IS NOT NULL
                    THEN ARRAY_TO_STRING(fdi.business_activities, '; ')
                  WHEN fdi.other_business_activity NOT IN (NULL, '') AND fdi.business_activities IS NULL
                    THEN fdi.other_business_activity
                  WHEN fdi.other_business_activity NOT IN (NULL, '') AND fdi.business_activities IS NOT NULL
                    THEN fdi.other_business_activity || '; ' || ARRAY_TO_STRING(fdi.business_activities, '; ')
                END AS business_activities,
                fdi.project_arrived_in_triage_on,
                fdi.proposal_deadline,
                CASE WHEN fdi.export_revenue THEN 'yes' ELSE 'no' END AS export_revenue,
                ARRAY_TO_STRING(fdi.strategic_drivers, '; ') as strategic_drivers,
                fdi.gross_value_added,
                fdi.gva_multiplier
            FROM datahub.investment_projects fdi
             LEFT JOIN companies_last_version inv ON fdi.investor_company_id = inv.id
             LEFT JOIN companies_last_version ukc ON fdi.uk_company_id = ukc.id
             LEFT JOIN datahub.advisers crm ON fdi.client_relationship_manager_id = crm.id
             LEFT JOIN datahub.teams crmt ON crm.team_id = crmt.id
             LEFT JOIN datahub.advisers paa ON fdi.project_assurance_adviser_id = paa.id
             LEFT JOIN datahub.teams paat ON paa.team_id = paat.id
             LEFT JOIN datahub.advisers pm ON fdi.project_manager_id = pm.id
             LEFT JOIN datahub.teams pmt ON pm.team_id = pmt.id
             LEFT JOIN datahub.advisers cre ON fdi.created_by_id = cre.id
             LEFT JOIN datahub.teams cret ON cre.team_id = cret.id
             LEFT JOIN datahub.advisers mod ON fdi.modified_by_id = mod.id
             LEFT JOIN datahub.teams modt ON mod.team_id = modt.id
             LEFT JOIN datahub.advisers acm ON inv.one_list_account_owner_id = acm.id
             LEFT JOIN datahub.teams acmt ON acm.team_id = acmt.id
             LEFT JOIN (
                SELECT investment_project_id, max(interaction_date)::text as date_of_latest_interaction
                FROM datahub.interactions i
                WHERE investment_project_id IS NOT NULL
                GROUP BY investment_project_id
             ) i ON fdi.id = i.investment_project_id
            WHERE (
                (fdi.actual_land_date >= '2018-04-01' OR fdi.estimated_land_date >= '2018-04-01')
                AND LOWER(fdi.status) IN ('ongoing', 'won')
            )
            OR (
                (fdi.modified_on BETWEEN (date_trunc('month', :run_date) - interval '1 year') and date_trunc('month', :run_date) + interval '1 month')
                AND LOWER(fdi.status) NOT IN ('ongoing', 'won')
            )
            ORDER BY fdi.actual_land_date, fdi.estimated_land_date ASC
        )
        SELECT DISTINCT ON (unique_id)
            actual_land_date, estimated_land_date, created_on, modified_on,
            date_of_latest_interaction, name, description,  project_reference,  total_investment,
            number_new_jobs, number_safeguarded_jobs, client_requirements, address_1, address_2,
            address_postcode, id, client_relationship_manager_id, client_relationship_manager_name,
            client_relationship_manager_team, account_manager_name, account_manager_team, fdi_type,
            investment_type, investor_company_id, investor_company_name,
            investor_company_comp_house_id, investor_company_headquarter_type,
            investor_company_company_tier, investor_company_sector, investor_company_address_1,
            investor_company_address_2, investor_company_uk_region, investor_company_country,
            investor_company_contact_name, investor_company_contact_marked_as_primary_contact,
            investor_company_contact_phone, investor_company_contact_email,
            investor_company_contact_accepts_dit_email_marketing, specific_programme, stage,
            project_assurance_adviser_name, project_assurance_adviser_team, project_manager_name,
            project_manager_team, sector, uk_company_id, uk_company_name, uk_company_comp_house_id,
            uk_company_sector, uk_company_address_1, uk_company_address_2, uk_company_postcode,
            uk_company_uk_region, uk_company_country, uk_company_contact_name,
            uk_company_contact_marked_as_primary_contact, uk_company_contact_phone,
            uk_company_contact_email, uk_company_contact_accepts_dit_email_marketing,
            likelihood_to_land, fdi_value, created_by_name, created_by_team, modified_by_name,
            modified_by_team, status, anonymous_description, associated_non_fdi_r_and_d_project_id,
            competing_countries, team_members, investor_type, level_of_involvement,
            foreign_equity_investment, government_assistance, r_and_d_budget,
            non_fdi_r_and_d_budget, new_tech_to_uk, average_salary, referral_source_activity,
            referral_source_activity_website, referral_source_activity_marketing, delivery_partners,
            possible_uk_regions, actual_uk_regions, business_activities, project_arrived_in_triage_on,
            proposal_deadline, export_revenue, strategic_drivers, gross_value_added, gva_multiplier
        FROM fdi_report f
    '''
