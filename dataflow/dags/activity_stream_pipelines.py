from typing import Dict, List, cast

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.activity_stream import fetch_from_activity_stream
from dataflow.utils import TableConfig, JSONType


class _ActivityStreamPipeline(_PipelineDAG):
    index: str
    query: dict

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-activity-stream-data",
            python_callable=fetch_from_activity_stream,
            provide_context=True,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.index,
                self.query,
            ],
        )


class ERPPipeline(_ActivityStreamPipeline):
    index = "objects"
    table_name = "erp_submissions"
    table_config = TableConfig(
        table_name="erp",
        field_mapping=[
            ("id", sa.Column("id", sa.String, primary_key=True)),
            ("url", sa.Column("url", sa.String, primary_key=True)),
            (
                ("dit:directoryFormsApi:Submission:Data", "choice_change_type"),
                sa.Column("choice_change_type", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "choice_change_comment"),
                sa.Column("choice_change_comment", sa.Text),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "commodity",
                    "commodity_code",
                ),
                sa.Column("commodity_code", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "commodity", "label"),
                sa.Column("commodity_label", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "company_name"),
                sa.Column("company_name", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "company_number"),
                sa.Column("company_number", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "company_type"),
                sa.Column("company_type", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "consumer_region"),
                sa.Column("consumer_region", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "consumer_type"),
                sa.Column("consumer_type", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "country"),
                sa.Column("country", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "email"),
                sa.Column("email", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "employees"),
                sa.Column("employees", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "employment_regions"),
                sa.Column("employment_regions", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "equivalent_uk_goods"),
                sa.Column("equivalent_uk_goods", sa.Boolean),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "equivalent_uk_goods_details",
                ),
                sa.Column("equivalent_uk_goods_details", sa.Text),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "family_name"),
                sa.Column("family_name", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "given_name"),
                sa.Column("given_name", sa.String),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "has_consumer_choice_changed",
                ),
                sa.Column("has_consumer_choice_changed", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_consumer_price_changed"),
                sa.Column("has_consumer_price_changed", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_market_price_changed"),
                sa.Column("has_market_price_changed", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_market_size_changed"),
                sa.Column("has_market_size_changed", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_other_changes"),
                sa.Column("has_other_changes", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_other_changes_type"),
                sa.Column("has_other_changes_type", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_price_changed"),
                sa.Column("has_price_changed", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "has_volume_changed"),
                sa.Column("has_volume_changed", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "import_countries"),
                sa.Column("import_countries", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "income_bracket"),
                sa.Column("income_bracket", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "imported_good_sector"),
                sa.Column("imported_good_sector", sa.String),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "imported_good_sector_details",
                ),
                sa.Column("imported_good_sector_details", sa.Text),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "imported_goods_makes_something_else",
                ),
                sa.Column("imported_goods_makes_something_else", sa.Boolean),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "market_price_change_comment",
                ),
                sa.Column("market_price_change_comment", sa.Text),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "market_price_changed_type"),
                sa.Column("market_price_changed_type", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "market_size"),
                sa.Column("market_size", sa.Integer),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "market_size_change_comment"),
                sa.Column("market_size_change_comment", sa.Text),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "market_size_changed_type"),
                sa.Column("market_size_changed_type", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "market_size_known"),
                sa.Column("market_size_known", sa.Boolean),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "organisation_name"),
                sa.Column("organisation_name", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "other_changes_comment"),
                sa.Column("other_changes_comment", sa.Text),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "other_metric_name"),
                sa.Column("other_metric_name", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "other_information"),
                sa.Column("other_information", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "price_changed_type"),
                sa.Column("price_changed_type", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "price_change_comment"),
                sa.Column("price_change_comment", sa.Text),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "production_cost_percentage"),
                sa.Column("production_cost_percentage", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_four_2018_sales_revenue",
                ),
                sa.Column("quarter_four_2018_sales_revenue", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_four_2018_sales_volume",
                ),
                sa.Column("quarter_four_2018_sales_volume", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_one_2019_sales_revenue",
                ),
                sa.Column("quarter_one_2019_sales_revenue", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_one_2019_sales_volume",
                ),
                sa.Column("quarter_one_2019_sales_volume", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_two_2019_sales_revenue",
                ),
                sa.Column("quarter_two_2019_sales_revenue", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_two_2019_sales_volume",
                ),
                sa.Column("quarter_two_2019_sales_volume", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_three_2019_sales_revenue",
                ),
                sa.Column("quarter_three_2019_sales_revenue", sa.Integer),
            ),
            (
                (
                    "dit:directoryFormsApi:Submission:Data",
                    "quarter_three_2019_sales_volume",
                ),
                sa.Column("quarter_three_2019_sales_volume", sa.Integer),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "sales_volume_unit"),
                sa.Column("sales_volume_unit", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "sector"),
                sa.Column("sector", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "tariff_quota"),
                sa.Column("tariff_quota", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "tariff_rate"),
                sa.Column("tariff_rate", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "turnover"),
                sa.Column("turnover", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "user_type"),
                sa.Column("user_type", sa.String),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "volume_changed_type"),
                sa.Column("volume_changed_type", sa.ARRAY(sa.String)),
            ),
            (
                ("dit:directoryFormsApi:Submission:Data", "volumes_change_comment"),
                sa.Column("volumes_change_comment", sa.Text),
            ),
        ],
    )

    query = {
        "bool": {
            "filter": [
                {
                    "term": {
                        "attributedTo.id": "dit:directoryFormsApi:SubmissionType:ExceptionalReviewProcedure"
                    }
                },
                {
                    "term": {
                        "attributedTo.type": "dit:directoryFormsApi:SubmissionAction:zendesk"
                    }
                },
            ]
        }
    }


class GreatGOVUKExportOpportunitiesPipeline(_ActivityStreamPipeline):
    name = "great-gov-uk-export-opportunitites"
    index = "objects"
    table_config = TableConfig(
        table_name="great_gov_uk_export_opportunities",
        field_mapping=[
            ("id", sa.Column("id", sa.String, primary_key=True)),
            ("name", sa.Column("name", sa.String, nullable=False)),
            ("url", sa.Column("url", sa.String, nullable=False)),
            ("content", sa.Column("content", sa.Text)),
            ("summary", sa.Column("summary", sa.Text)),
            ("endTime", sa.Column("end_time", sa.DateTime)),
            ("dit:country", sa.Column("countries", sa.ARRAY(sa.String))),
            (
                "dit:exportOpportunities:Opportunity:id",
                sa.Column("opportunity_id", UUID(as_uuid=True)),
            ),
        ],
    )

    query = {"bool": {"filter": [{"term": {"type": "dit:Opportunity"}}]}}


class GreatGOVUKExportOpportunityEnquiriesPipeline(_ActivityStreamPipeline):
    name = "great-gov-uk-export-opportunity-enquiries"
    index = "activities"
    table_config = TableConfig(
        table_name="great_gov_uk_export_opportunity_enquiries",
        field_mapping=[
            (("object", "id"), sa.Column("id", sa.String, primary_key=True)),
            (("object", "published"), sa.Column("published", sa.DateTime)),
            (
                ("actor", 0, "name"),
                sa.Column("company_name", sa.String, nullable=False),
            ),
            (("actor", 0, "url"), sa.Column("company_url", sa.String)),
            (
                ("actor", 0, "dit:companiesHouseNumber"),
                sa.Column("company_number", sa.String),
            ),
            (
                ("actor", 0, "dit:companyIsExistingExporter"),
                sa.Column("is_existing_exporter", sa.String),
            ),
            (
                ("actor", 0, "dit:phoneNumber"),
                sa.Column("company_phone_number", sa.String),
            ),
            (("actor", 0, "dit:sector"), sa.Column("sector", sa.String)),
            (
                ("actor", 0, "location", "dit:postcode"),
                sa.Column("postcode", sa.String),
            ),
            (
                ("actor", 1, "dit:emailAddress"),
                sa.Column("contact_email_address", sa.String),
            ),
            (("actor", 1, "name"), sa.Column("contact_name", sa.ARRAY(sa.String))),
            (("object", "url"), sa.Column("url", sa.String)),
            (
                ("object", "inReplyTo", "dit:exportOpportunities:Opportunity:id"),
                sa.Column("opportunity_id", UUID(as_uuid=True)),
            ),
        ],
    )

    query = {
        "bool": {
            "filter": [
                {"term": {"object.type": "dit:exportOpportunities:Enquiry"}},
                {"term": {"type": "Create"}},
            ]
        }
    }


class LITECasesPipeline(_ActivityStreamPipeline):
    name = "lite-cases"
    index = "activities"
    table_config = TableConfig(
        table_name="lite_cases",
        field_mapping=[
            (("object", "id"), sa.Column("id", sa.String, primary_key=True)),
            (("object", "dit:caseOfficer"), sa.Column("case_officer", sa.String)),
            (("object", "dit:countries"), sa.Column("countries", sa.ARRAY(sa.String))),
            (("object", "dit:status"), sa.Column("status", sa.String)),
            (("object", "dit:submittedDate"), sa.Column("submitted_date", sa.DateTime)),
            (("object", "type"), sa.Column("type", sa.ARRAY(sa.String))),
        ],
    )

    query = {"bool": {"filter": [{"term": {"object.type": "dit:lite:case"}}]}}


class LITECaseChangesPipeline(_ActivityStreamPipeline):
    name = "lite-case-changes"
    index = "activities"
    table_config = TableConfig(
        table_name="lite_case_changes",
        field_mapping=[
            (("object", "id"), sa.Column("id", sa.String, primary_key=True)),
            (("object", "dit:from"), sa.Column("case_from", sa.JSON)),
            (("object", "dit:to"), sa.Column("case_to", sa.JSON)),
            (("object", "attributedTo"), sa.Column("attributed_to", sa.JSON)),
            (("object", "type"), sa.Column("type", sa.ARRAY(sa.String))),
        ],
    )

    query = {"bool": {"filter": [{"term": {"object.type": "dit:lite:case:change"}}]}}


def staff_sso_users_get_app_names(apps: JSONType) -> JSONType:
    # Happy with a runtime error if apps is is not a list, since the data would not
    # be of the expected structure
    return [app["name"] for app in cast(List[Dict[str, str]], apps)]


class StaffSSOUsersPipeline(_ActivityStreamPipeline):
    name = "staff-sso-users"
    index = "objects"
    table_config = TableConfig(
        schema="dit",
        table_name="staff_sso__users",
        field_mapping=[
            (
                "dit:StaffSSO:User:userId",
                sa.Column("user_id", UUID(as_uuid=True), primary_key=True),
            ),
            ("dit:emailAddress", sa.Column("email", sa.ARRAY(sa.String), index=True)),
            (
                "dit:StaffSSO:User:contactEmailAddress",
                sa.Column("contact_email", sa.String, index=True),
            ),
            ("dit:firstName", sa.Column("first_name", sa.String)),
            ("dit:lastName", sa.Column("last_name", sa.String)),
            ("dit:StaffSSO:User:joined", sa.Column("joined", sa.DateTime)),
            ("dit:StaffSSO:User:lastAccessed", sa.Column("last_accessed", sa.DateTime)),
            (
                (
                    "dit:StaffSSO:User:permittedApplications",
                    staff_sso_users_get_app_names,
                ),
                sa.Column("permitted_applications", sa.ARRAY(sa.String)),
            ),
        ],
    )

    query = {"bool": {"filter": [{"term": {"type": "dit:StaffSSO:User"}}]}}


class GreatGovUKFormsPipeline(_ActivityStreamPipeline):
    name = "great-gov-uk-forms"
    index = "activities"

    table_config = TableConfig(
        schema="dit",
        table_name="great_gov_uk__forms",
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                "norm_id": record["object"]["id"].replace(
                    "dit:directoryFormsApi:Submission:", ""
                ),
                "submission_type": record["object"]["attributedTo"]["id"].replace(
                    "dit:directoryFormsApi:SubmissionType:", ""
                ),
                "submission_action": record["object"]["attributedTo"]["type"].replace(
                    "dit:directoryFormsApi:SubmissionAction:", ""
                ),
            }
        ],
        field_mapping=[
            ("norm_id", sa.Column("id", sa.Integer, primary_key=True)),
            (("object", "url"), sa.Column("url", sa.String)),
            (("object", "published"), sa.Column("created_at", sa.DateTime)),
            ("submission_type", sa.Column("submission_type", sa.String)),
            ("submission_action", sa.Column("submission_action", sa.String)),
            (("actor", "dit:emailAddress"), sa.Column("actor_email", sa.String)),
            (("actor"), sa.Column("actor", sa.JSON),),
            (
                ("object", "dit:directoryFormsApi:Submission:Data"),
                sa.Column("data", sa.JSON),
            ),
            (
                ("object", "dit:directoryFormsApi:Submission:Meta"),
                sa.Column("meta", sa.JSON),
            ),
        ],
    )

    query = {
        "bool": {
            "filter": [
                {"term": {"object.type": "dit:directoryFormsApi:Submission"}},
                {"term": {"type": "Create"}},
            ]
        }
    }


class ReturnToOfficeBookingsPipeline(_ActivityStreamPipeline):
    index = "objects"
    table_config = TableConfig(
        schema="dit",
        table_name="return_to_office__bookings",
        field_mapping=[
            (
                "dit:ReturnToOffice:Booking:bookingId",
                sa.Column("id", sa.Integer, primary_key=True),
            ),
            ("dit:ReturnToOffice:Booking:userId", sa.Column("user_id", sa.Integer)),
            ("dit:ReturnToOffice:Booking:userEmail", sa.Column("user_email", sa.Text)),
            (
                "dit:ReturnToOffice:Booking:userFullName",
                sa.Column("user_name", sa.Text),
            ),
            (
                "dit:ReturnToOffice:Booking:onBehalfOfName",
                sa.Column("on_behalf_of_name", sa.Text),
            ),
            (
                "dit:ReturnToOffice:Booking:onBehalfOfEmail",
                sa.Column("on_behalf_of_email", sa.Text),
            ),
            (
                "dit:ReturnToOffice:Booking:bookingDate",
                sa.Column("booking_date", sa.Date),
            ),
            (
                "dit:ReturnToOffice:Booking:building",
                sa.Column("building_name", sa.Text),
            ),
            ("dit:ReturnToOffice:Booking:floor", sa.Column("floor_name", sa.Text)),
            (
                "dit:ReturnToOffice:Booking:directorate",
                sa.Column("directorate", sa.Text),
            ),
            ("dit:ReturnToOffice:Booking:group", sa.Column("group", sa.Text),),
            (
                "dit:ReturnToOffice:Booking:businessUnit",
                sa.Column("business_unit", sa.Text),
            ),
            ("dit:ReturnToOffice:Booking:created", sa.Column("created", sa.DateTime)),
            (
                "dit:ReturnToOffice:Booking:cancelled",
                sa.Column("cancelled", sa.DateTime),
            ),
        ],
    )

    query = {"bool": {"filter": [{"term": {"type": "dit:ReturnToOffice:Booking"}}]}}


class MaxemailCampaignsSentPipeline(_ActivityStreamPipeline):
    name = "maxemail-campaigns-sent"
    index = "activities"
    table_config = TableConfig(
        schema="dit",
        table_name="maxemail__email_campaigns_sent",
        field_mapping=[
            (("object", "id"), sa.Column("id", sa.String, primary_key=True)),
            (("object", "dit:emailAddress"), sa.Column("email_address", sa.String)),
            (("object", "attributedTo", "name"), sa.Column("campaign_name", sa.String)),
            (("object", "attributedTo", "published"), sa.Column("sent", sa.DateTime)),
            (("object", "type"), sa.Column("type", sa.ARRAY(sa.String))),
        ],
    )

    query = {"bool": {"filter": [{"term": {"object.type": "dit:maxemail:Email"}}]}}
