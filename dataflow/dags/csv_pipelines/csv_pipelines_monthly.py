from datetime import datetime

from dataflow.dags.csv_pipeline import BaseCSVPipeline


class BaseMonthlyCSVPipeline(BaseCSVPipeline):
    """
    Base DAG to allow subclasses to be picked up by airflow
    """

    schedule_interval = '0 5 1 * *'
    start_date = datetime(2019, 10, 1)


class DataHubOMISCompletedOrdersCSVPipeline(BaseMonthlyCSVPipeline):
    """Pipeline meta object for Completed OMIS Order CSV."""

    base_file_name = 'completed_omis_orders'
    query = '''
        SELECT
            omis_dataset.omis_order_reference AS "OMIS Order Reference",
            companies_dataset.name AS "Company name",
            teams_dataset.name AS "DIT Team",
            ROUND(omis_dataset.subtotal::numeric/100::numeric,2) AS "Net price",
            omis_dataset.uk_region AS "UK Region",
            omis_dataset.market AS "Market",
            omis_dataset.sector AS "Sector",
            omis_dataset.services AS "Services",
            to_char(omis_dataset.delivery_date, 'DD/MM/YYYY') AS "Delivery date",
            to_char(omis_dataset.payment_received_date, 'DD/MM/YYYY') AS "Payment received date",
            to_char(omis_dataset.completion_date, 'DD/MM/YYYY') AS "Completion Date",
            to_char(omis_dataset.created_date, 'DD/MM/YYYY') AS "Created date",
            to_char(omis_dataset.cancelled_date, 'DD/MM/YYYY') AS "Cancelled date",
            omis_dataset.cancellation_reason AS "Cancellation reason",
            to_char(omis_dataset.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis_dataset.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis_dataset.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis_dataset.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis_dataset.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM omis_dataset
        LEFT JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        LEFT JOIN teams_dataset ON omis_dataset.dit_team_id=teams_dataset.id
        WHERE omis_dataset.order_status = 'complete'
        AND date_trunc('month', omis_dataset.completion_date) = date_trunc('month', :run_date)
        ORDER BY omis_dataset.completion_date
        '''


class DataHubOMISCancelledOrdersCSVPipeline(BaseMonthlyCSVPipeline):
    """Pipeline meta object for Cancelled OMIS Order CSV."""

    base_file_name = 'cancelled_omis_orders'
    query = '''
        WITH omis AS (
            SELECT
                CASE WHEN EXTRACT('month' FROM cancelled_date)::int >= 4
                    THEN (to_char(omis_dataset.cancelled_date, 'YYYY')::int)
                    ELSE (to_char(omis_dataset.cancelled_date + interval '-1' year, 'YYYY')::int)
                END as cancelled_date_financial_year,
                CASE WHEN EXTRACT('month' FROM CURRENT_DATE)::int >= 4
                    THEN (to_char(CURRENT_DATE, 'YYYY')::int)
                    ELSE (to_char(CURRENT_DATE + interval '-1' year, 'YYYY')::int)
                END as current_financial_year,
                *
            FROM omis_dataset
        )
        SELECT
            omis.omis_order_reference AS "OMIS Order Reference",
            companies_dataset.name AS "Company Name",
            ROUND(omis.subtotal::numeric/100::numeric,2) AS "Net price",
            teams_dataset.name AS "DIT Team",
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
        LEFT JOIN companies_dataset ON omis.company_id=companies_dataset.id
        LEFT JOIN teams_dataset ON omis.dit_team_id=teams_dataset.id
        WHERE omis.order_status = 'cancelled'
        AND omis.cancelled_date_financial_year = omis.current_financial_year
    '''


class DataHubOMISAllOrdersCSVPipeline(BaseMonthlyCSVPipeline):
    """View pipeline for all OMIS orders created up to the end
     of the last calendar month"""

    base_file_name = 'all_omis_orders'
    start_date = datetime(2019, 12, 1)
    query = '''
        SELECT
            omis_dataset.omis_order_reference AS "Order ID",
            omis_dataset.order_status AS "Order status",
            companies_dataset.name AS "Company",
            teams_dataset.name AS "Creator team",
            omis_dataset.uk_region AS "UK region",
            omis_dataset.market AS "Primary market",
            omis_dataset.sector AS "Sector",
            companies_dataset.sector AS "Company sector",
            omis_dataset.net_price AS "Net price",
            omis_dataset.services AS "Services",
            TO_CHAR(omis_dataset.created_date, 'YYYY-MM-DD')::DATE AS "Order created",
            TO_CHAR(omis_dataset.quote_created_on, 'YYYY-MM-DD')::DATE AS "Quote created",
            TO_CHAR(omis_dataset.quote_accepted_on, 'YYYY-MM-DD')::DATE AS "Quote accepted",
            TO_CHAR(omis_dataset.delivery_date, 'YYYY-MM-DD')::DATE AS "Planned delivery date",
            omis_dataset.vat_cost AS "VAT",
            TO_CHAR(omis_dataset.payment_received_date, 'YYYY-MM-DD')::DATE AS "Payment received date",
            TO_CHAR(omis_dataset.completion_date, 'YYYY-MM-DD')::DATE AS "Completion date",
            TO_CHAR(omis_dataset.cancelled_date, 'YYYY-MM-DD')::DATE AS "Cancellation date",
            omis_dataset.refund_created AS "Refund date",
            omis_dataset.refund_total_amount AS "Refund amount"
        FROM omis_dataset
        JOIN companies_dataset ON omis_dataset.company_id = companies_dataset.id
        JOIN teams_dataset on omis_dataset.dit_team_id = teams_dataset.id
        WHERE omis_dataset.created_date < date_trunc('month', :run_date)
    '''


class DataHubOMISClientSurveyStaticCSVPipeline(BaseMonthlyCSVPipeline):
    """Pipeline meta object for monthly OMIS Client Survey report."""

    base_file_name = 'omis_client_survey_static'
    static = True
    query = '''
        SELECT
            companies_dataset.name AS "Company Name",
            contacts_dataset.contact_name AS "Contact Name",
            contacts_dataset.phone AS "Contact Phone Number",
            contacts_dataset.email AS "Contact Email",
            companies_dataset.address_1 AS "Company Trading Address Line 1",
            companies_dataset.address_2 AS "Company Trading Address Line 2",
            companies_dataset.address_town AS "Company Trading Address Town",
            companies_dataset.address_county AS "Company Trading Address County",
            companies_dataset.address_country AS "Company Trading Address Country",
            companies_dataset.address_postcode AS "Company Trading Address Postcode",
            companies_dataset.registered_address_1 AS "Company Registered Address Line 1",
            companies_dataset.registered_address_2 AS "Company Registered Address Line 2",
            companies_dataset.registered_address_town AS "Company Registered Address Town",
            companies_dataset.registered_address_county AS "Company Registered Address County",
            companies_dataset.registered_address_country AS "Company Registered Address Country",
            companies_dataset.registered_address_postcode AS "Company Registered Address Postcode",
            to_char(omis_dataset.refund_created, 'DD/MM/YYYY') AS "Date of Refund",
            (omis_dataset.refund_total_amount/100)::numeric(15, 2) AS "Refund Amount",
            (omis_dataset.vat_cost/100)::numeric(15, 2) AS "VAT Amount",
            (omis_dataset.total_cost/100)::numeric(15, 2) AS "Gross Amount",
            to_char(omis_dataset.quote_created_on, 'DD/MM/YYYY') AS "Quote Created"
        FROM omis_dataset
        JOIN companies_dataset ON omis_dataset.company_id=companies_dataset.id
        JOIN contacts_dataset ON omis_dataset.contact_id=contacts_dataset.id
        WHERE omis_dataset.order_status = 'complete'
        AND date_trunc('month', omis_dataset.completion_date) = date_trunc('month', :run_date)
        ORDER BY omis_dataset.completion_date
    '''


class DataHubServiceDeliveryInteractionsCSVPipeline(BaseMonthlyCSVPipeline):
    """Pipeline meta object for the data hub service deliveries and interactions report."""

    base_file_name = 'datahub_service_interactions'
    start_date = datetime(2019, 11, 15)
    schedule_interval = '0 5 15 * *'
    query = '''
        WITH interactions AS (
            SELECT *
            FROM interactions_dataset
            WHERE interactions_dataset.interaction_kind = 'service_delivery'
            AND date_trunc('month', interactions_dataset.interaction_date) = date_trunc('month', :run_date)
        ),
        contact_ids AS (
            SELECT id AS interaction_id, UNNEST(contact_ids)::uuid AS contact_id
            FROM interactions
        ),
        contacts AS (
            SELECT DISTINCT ON (contact_ids.interaction_id) *
            FROM contacts_dataset
            JOIN contact_ids ON contacts_dataset.id = contact_ids.contact_id
            ORDER BY contact_ids.interaction_id, contacts_dataset.is_primary DESC NULLS LAST
        ),
        adviser_ids AS (
            SELECT id AS interaction_id, UNNEST(adviser_ids)::uuid AS adviser_id
            FROM interactions_dataset
        ),
        team_names AS (
            SELECT adviser_ids.interaction_id as iid, STRING_AGG(teams_dataset.name, '; ') AS names
            FROM advisers_dataset
            JOIN adviser_ids ON advisers_dataset.id = adviser_ids.adviser_id
            JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
            GROUP BY 1
        )
        SELECT
            to_char(interactions.interaction_date, 'DD/MM/YYYY') AS "Date of Interaction",
            interactions.interaction_kind AS "Interaction Type",
            companies_dataset.name AS "Company Name",
            companies_dataset.company_number AS "Companies HouseID",
            companies_dataset.id AS "Data Hub Company ID",
            companies_dataset.cdms_reference_code AS "CDMS Reference Code",
            companies_dataset.address_postcode AS "Company Postcode",
            companies_dataset.address_1 AS "Company Address Line 1",
            companies_dataset.address_2 AS "Company Address Line 2",
            companies_dataset.address_town AS "Company Address Town",
            companies_dataset.address_country AS "Company Address Country",
            companies_dataset.website AS "Company Website",
            companies_dataset.number_of_employees AS "Number of Employees",
            companies_dataset.is_number_of_employees_estimated AS "Number of Employees Estimated",
            companies_dataset.turnover AS "Turnover",
            companies_dataset.is_turnover_estimated AS "Turnover Estimated",
            companies_dataset.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            advisers_dataset.first_name AS "DIT Adviser First Name",
            advisers_dataset.last_name AS "DIT Adviser Last Name",
            advisers_dataset.telephone_number AS "DIT Adviser Phone",
            advisers_dataset.contact_email AS "DIT Adviser Email",
            team_names.names AS "DIT Team",
            companies_dataset.uk_region AS "Company UK Region",
            interactions.service_delivery AS "Service Delivery",
            interactions.interaction_subject AS "Subject",
            interactions.interaction_notes AS "Notes",
            interactions.net_company_receipt AS "Net Company Receipt",
            interactions.grant_amount_offered AS "Grant Amount Offered",
            interactions.service_delivery_status AS "Service Delivery Status",
            events_dataset.name AS "Event Name",
            events_dataset.event_type AS "Event Type",
            to_char(events_dataset.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            events_dataset.address_town AS "Event Town",
            events_dataset.address_country AS "Event Country",
            events_dataset.uk_region AS "Event UK Region",
            events_dataset.service_name AS "Event Service Name",
            to_char(interactions.created_on, 'DD/MM/YYYY') AS "Created On Date",
            interactions.communication_channel AS "Communication Channel",
            interactions.interaction_link AS "Interaction Link"
        FROM interactions
        JOIN companies_dataset ON interactions.company_id = companies_dataset.id
        JOIN advisers_dataset ON interactions.adviser_ids[1]::uuid = advisers_dataset.id
        LEFT JOIN events_dataset ON interactions.event_id = events_dataset.id
        LEFT JOIN contacts ON contacts.interaction_id = interactions.id
        LEFT JOIN team_names ON team_names.iid = interactions.id
        ORDER BY interactions.interaction_date
    '''


class DataHubExportClientSurveyStaticCSVPipeline(BaseMonthlyCSVPipeline):
    """Pipeline meta object for the data hub export client survey report."""

    base_file_name = 'datahub_export_client_survey'
    start_date = datetime(2019, 11, 15)
    schedule_interval = '0 5 15 * *'
    static = True
    query = '''
        WITH service_deliveries AS (
            SELECT *
            FROM interactions_dataset
            WHERE interactions_dataset.interaction_kind = 'service_delivery'
            AND date_trunc('month', interactions_dataset.interaction_date) = date_trunc('month', :run_date)
        ),
        contact_ids AS (
            SELECT id AS service_delivery_id, UNNEST(contact_ids)::uuid AS contact_id
                FROM service_deliveries
        ),
        contacts AS (
                SELECT DISTINCT ON (contact_ids.service_delivery_id) *
                FROM contacts_dataset
                JOIN contact_ids ON contacts_dataset.id = contact_ids.contact_id
                ORDER BY contact_ids.service_delivery_id, contacts_dataset.is_primary DESC NULLS LAST
        ),
        adviser_ids AS (
            SELECT id AS service_delivery_id, UNNEST(adviser_ids)::uuid AS adviser_id
            FROM service_deliveries
        ),
        team_names AS (
            SELECT adviser_ids.service_delivery_id as sid, STRING_AGG(teams_dataset.name, '; ') AS names
            FROM advisers_dataset
            JOIN adviser_ids ON advisers_dataset.id = adviser_ids.adviser_id
            JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
            GROUP BY 1
        ),
        team_roles AS (
            SELECT adviser_ids.service_delivery_id as sid, STRING_AGG(teams_dataset.role, '; ') AS roles
            FROM advisers_dataset
            JOIN adviser_ids ON advisers_dataset.id = adviser_ids.adviser_id
            JOIN teams_dataset ON advisers_dataset.team_id = teams_dataset.id
            GROUP BY 1
        )
        SELECT
            to_char(service_deliveries.interaction_date, 'DD/MM/YYYY') AS "Service Delivery Interaction",
            companies_dataset.name AS "Company Name",
            companies_dataset.company_number as "Companies House ID",
            companies_dataset.id AS "Data Hub Company ID",
            companies_dataset.cdms_reference_code AS "CDMS Reference Code",
            companies_dataset.address_postcode AS "Company Postcode",
            companies_dataset.company_number AS "Companies HouseID",
            companies_dataset.address_1 AS "Company Address Line 1",
            companies_dataset.address_2 AS "Company Address Line 2",
            companies_dataset.address_town AS "Company Address Town",
            companies_dataset.address_country AS "Company Address Country",
            companies_dataset.website AS "Company Website",
            companies_dataset.number_of_employees AS "Number of Employees",
            companies_dataset.is_number_of_employees_estimated AS "Number of Employees Estimated",
            companies_dataset.turnover AS "Turnover",
            companies_dataset.is_turnover_estimated AS "Turnover Estimated",
            companies_dataset.sector AS "Sector",
            contacts.contact_name AS "Contact Name",
            contacts.phone AS "Contact Phone",
            contacts.email AS "Contact Email",
            contacts.address_postcode AS "Contact Postcode",
            contacts.address_1 AS "Contact Address Line 1",
            contacts.address_2 AS "Contact Address Line 2",
            contacts.address_town AS "Contact Address Town",
            contacts.address_country AS "Contact Address Country",
            team_names.names AS "DIT Team",
            companies_dataset.uk_region AS "Company UK Region",
            service_deliveries.service_delivery AS "Service Delivery",
            service_deliveries.interaction_subject AS "Subject",
            service_deliveries.interaction_notes AS "Notes",
            service_deliveries.net_company_receipt AS "Net Company Receipt",
            service_deliveries.grant_amount_offered AS "Grant Amount Offered",
            service_deliveries.service_delivery_status AS "Service Delivery Status",
            events_dataset.name AS "Event Name",
            events_dataset.event_type AS "Event Type",
            to_char(events_dataset.start_date, 'DD/MM/YYYY') AS "Event Start Date",
            events_dataset.address_town AS "Event Town",
            events_dataset.address_country AS "Event Country",
            events_dataset.uk_region AS "Event UK Region",
            events_dataset.service_name AS "Event Service Name",
            team_roles.roles AS "Team Role",
            to_char(service_deliveries.created_on, 'DD/MM/YYYY') AS "Created On Date"
        FROM service_deliveries
        JOIN companies_dataset ON service_deliveries.company_id = companies_dataset.id
        LEFT JOIN events_dataset ON service_deliveries.event_id = events_dataset.id
        LEFT JOIN contacts ON contacts.service_delivery_id = service_deliveries.id
        LEFT JOIN team_names ON team_names.sid = service_deliveries.id
        LEFT JOIN team_roles ON team_roles.sid = service_deliveries.id
        ORDER BY service_deliveries.interaction_date
    '''


class DataHubFDIMonthlyStaticCSVPipeline(BaseMonthlyCSVPipeline):
    """Static monthly view of the FDI (investment projects) report"""

    base_file_name = 'data_hub_fdi_monthly_static'
    start_date = datetime(2020, 1, 1)
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
                inv.classification AS investor_company_company_tier,
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
                  WHEN fdi.other_business_activity IS NULL
                       AND fdi.business_activities IS NOT NULL
                    THEN fdi.business_activities
                  WHEN fdi.other_business_activity IS NOT NULL
                       AND fdi.business_activities IS NULL
                    THEN fdi.other_business_activity
                  WHEN fdi.other_business_activity IS NOT NULL
                       AND fdi.business_activities IS NOT NULL
                    THEN fdi.other_business_activity || ', ' || fdi.business_activities
                  ELSE NULL END AS business_activities,
                fdi.project_arrived_in_triage_on,
                fdi.proposal_deadline,
                CASE WHEN fdi.export_revenue THEN 'yes' ELSE 'no' END AS export_revenue,
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
            WHERE (
                (fdi.actual_land_date >= '2018-04-01' OR fdi.estimated_land_date >= '2018-04-01')
                AND LOWER(fdi.status) IN ('ongoing', 'won')
            )
            OR (
                (fdi.modified_on BETWEEN (date_trunc('month', :run_date) - interval '1 year') and date_trunc('month', :run_date))
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


for pipeline in BaseMonthlyCSVPipeline.__subclasses__():
    globals()[pipeline.__name__ + '__dag_1'] = pipeline().get_dag()
