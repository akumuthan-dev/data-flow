from functools import partial

import base64
import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_api_endpoint
from dataflow.operators.zendesk import fetch_daily_tickets
from dataflow.utils import TableConfig


def transforms_fields_dit(record, table_config, contexts):
    """
    Transform data export fields to match those of the DIT Zendesk API.
    """

    source = (record.get("via") or {}).get("source") or {}
    source.pop("from", None)
    record["via"]["source"] = source

    for name, id in [
        ["e_responder", 360000214018],
        ["e_sector", 360000211237],
        ["service", 360012120972],
        ["e_policy_area", 360000213978],
        ["e_dit_old", 360000213998],
    ]:
        record[name] = next(f for f in record["custom_fields"] if f["id"] == id)[
            "value"
        ]

    if "assignee" in record:
        return {
            **record,
            "assignee_id": record.get("assignee")["id"]
            if record.get("assignee")
            else None,
            "group_id": record.get("group")["id"] if record.get("group") else None,
            "organization_id": record["organization"]["id"]
            if record.get("organization")
            else None,
            "solved_at": record["metric_set"]["solved_at"],
            "full_resolution_time_in_minutes": record["metric_set"][
                "full_resolution_time_in_minutes"
            ]["calendar"],
        }

    return record


def transforms_fields_uktrade(record, table_config, contexts):
    """
    Transform data export fields to match those of the UKTrade Zendesk API.
    """

    service = next(
        field for field in record["custom_fields"] if field["id"] == 31281329
    )
    if "requester" in record:
        org = record['organization']
        return {
            **record,
            "submitter_id": record["submitter"]["id"],
            "requester_id": record["requester"]["id"],
            "assignee_id": record.get("assignee")["id"]
            if record.get("assignee")
            else None,
            "group_id": record.get("group")["id"] if record.get("group") else None,
            "collaborator_ids": [
                collaborator["id"] for collaborator in record["collaborator"]
            ],
            "organization_id": org['id'] if isinstance(org, dict) else org,
            "service": service["value"],
            "solved_at": record["metric_set"]["solved_at"],
            "full_resolution_time_in_minutes": record["metric_set"][
                "full_resolution_time_in_minutes"
            ]["calendar"],
        }
    return {**record, "service": service["value"]}


class ZendeskDITTicketsPipeline(_PipelineDAG):
    schedule_interval = "@daily"
    allow_null_columns = True
    use_utc_now_as_source_modified = True
    table_config = TableConfig(
        schema="zendesk",
        table_name="dit_tickets",
        transforms=[transforms_fields_dit],
        field_mapping=[
            ("id", sa.Column("id", sa.BigInteger, primary_key=True)),
            ("via", sa.Column("via", sa.JSON)),
            ("created_at", sa.Column("created_at", sa.DateTime)),
            ("solved_at", sa.Column("solved_at", sa.DateTime)),
            ("updated_at", sa.Column("updated_at", sa.DateTime)),
            (
                "full_resolution_time_in_minutes",
                sa.Column("full_resolution_time_in_minutes", sa.Integer),
            ),
            ("subject", sa.Column("subject", sa.String)),
            ("description", sa.Column("description", sa.String)),
            ("service", sa.Column("service", sa.String)),
            ("priority", sa.Column("priority", sa.String)),
            ("status", sa.Column("status", sa.String)),
            ("recipient", sa.Column("recipient", sa.String)),
            ("assignee_id", sa.Column("assignee_id", sa.BigInteger)),
            ("organization_id", sa.Column("organization_id", sa.BigInteger)),
            ("group_id", sa.Column("group_id", sa.BigInteger)),
            ("tags", sa.Column("tags", sa.ARRAY(sa.String))),
            ("satisfaction_rating", sa.Column("satisfaction_rating", sa.JSON)),
            (
                "sharing_agreement_ids",
                sa.Column("sharing_agreement_ids", sa.ARRAY(sa.BigInteger)),
            ),
            ("brand_id", sa.Column("brand_id", sa.BigInteger)),
            ("service", sa.Column("service", sa.String)),
            ("e_responder", sa.Column("e_responder", sa.ARRAY(sa.String))),
            ("e_sector", sa.Column("e_sector", sa.ARRAY(sa.String))),
            ("e_policy_area", sa.Column("e_policy_area", sa.ARRAY(sa.String))),
            ("e_dit_old", sa.Column("e_dit_old", sa.String)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-daily-tickets",
            python_callable=fetch_daily_tickets,
            provide_context=True,
            op_args=[self.table_config.schema, self.table_config.table_name, "dit"],
            retries=self.fetch_retries,
        )


class ZendeskUKTradeTicketsPipeline(_PipelineDAG):
    schedule_interval = "@daily"
    allow_null_columns = True
    use_utc_now_as_source_modified = True
    table_config = TableConfig(
        schema="zendesk",
        table_name="uktrade_tickets",
        transforms=[transforms_fields_uktrade],
        field_mapping=[
            ("id", sa.Column("id", sa.BigInteger, primary_key=True)),
            ("via", sa.Column("via", sa.JSON)),
            ("created_at", sa.Column("created_at", sa.DateTime)),
            ("solved_at", sa.Column("solved_at", sa.DateTime)),
            ("updated_at", sa.Column("updated_at", sa.DateTime)),
            (
                "full_resolution_time_in_minutes",
                sa.Column("full_resolution_time_in_minutes", sa.Integer),
            ),
            ("subject", sa.Column("subject", sa.String)),
            ("description", sa.Column("description", sa.String)),
            ("service", sa.Column("service", sa.String)),
            ("priority", sa.Column("priority", sa.String)),
            ("status", sa.Column("status", sa.String)),
            ("recipient", sa.Column("recipient", sa.String)),
            ("requester_id", sa.Column("requester_id", sa.BigInteger)),
            ("submitter_id", sa.Column("submitter_id", sa.BigInteger)),
            ("assignee_id", sa.Column("assignee_id", sa.BigInteger)),
            ("organization_id", sa.Column("organization_id", sa.BigInteger)),
            ("group_id", sa.Column("group_id", sa.BigInteger)),
            (
                "collaborator_ids",
                sa.Column("collaborator_ids", sa.ARRAY(sa.BigInteger)),
            ),
            ("follower_ids", sa.Column("follower_ids", sa.ARRAY(sa.BigInteger))),
            ("email_cc_ids", sa.Column("email_cc_ids", sa.ARRAY(sa.BigInteger))),
            ("has_incidents", sa.Column("has_incidents", sa.Boolean)),
            ("is_public", sa.Column("is_public", sa.Boolean)),
            ("due_at", sa.Column("due_at", sa.DateTime)),
            ("tags", sa.Column("tags", sa.ARRAY(sa.String))),
            ("satisfaction_rating", sa.Column("satisfaction_rating", sa.JSON)),
            (
                "sharing_agreement_ids",
                sa.Column("sharing_agreement_ids", sa.ARRAY(sa.BigInteger)),
            ),
            ("brand_id", sa.Column("brand_id", sa.BigInteger)),
            ("allow_channelback", sa.Column("allow_channelback", sa.Boolean)),
            ("allow_attachments", sa.Column("allow_attachments", sa.Boolean)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-daily-tickets",
            python_callable=fetch_daily_tickets,
            provide_context=True,
            op_args=[
                self.table_config.schema,
                self.table_config.table_name,
                "uktrade",
            ],
            retries=self.fetch_retries,
        )


class ZendeskUKTRadeGroupsPipeline(_PipelineDAG):
    schedule_interval = "0 0 * * MON"
    allow_null_columns = True
    use_utc_as_new_source_modified = True
    table_config = TableConfig(
        schema="zendesk",
        table_name="uktrade_groups",
        field_mapping=[
            ("id", sa.Column("id", sa.BigInteger, primary_key=True)),
            ("name", sa.Column("name", sa.String)),
            ("description", sa.Column("description", sa.String)),
            ("default", sa.Column("default", sa.Boolean)),
            ("deleted", sa.Column("deleted", sa.Boolean)),
            ("created_at", sa.Column("created_at", sa.DateTime)),
            ("updated_at", sa.Column("updated_at", sa.DateTime)),
        ],
    )
    source_url = f"{config.ZENDESK_CREDENTIALS['uktrade']['url']}/groups.json"
    email = config.ZENDESK_CREDENTIALS["uktrade"]["email"]
    secret = config.ZENDESK_CREDENTIALS["uktrade"]["secret"]
    auth_token = base64.b64encode(f"{email}/token:{secret}".encode()).decode()

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="run-fetch",
            python_callable=partial(
                fetch_from_api_endpoint,
                auth_token=self.auth_token,
                results_key='groups',
                next_key='next_page',
                auth_type="Basic",
                extra_headers={"Content-Type": "application/json"},
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
            retries=self.fetch_retries,
        )


class ZendeskDITGroupsPipeline(_PipelineDAG):
    schedule_interval = "0 0 * * MON"
    allow_null_columns = True
    use_utc_as_new_source_modified = True
    table_config = TableConfig(
        schema="zendesk",
        table_name="dit_groups",
        field_mapping=[
            ("id", sa.Column("id", sa.BigInteger, primary_key=True)),
            ("name", sa.Column("name", sa.String)),
            ("description", sa.Column("description", sa.String)),
            ("default", sa.Column("default", sa.Boolean)),
            ("deleted", sa.Column("deleted", sa.Boolean)),
            ("created_at", sa.Column("created_at", sa.DateTime)),
            ("updated_at", sa.Column("updated_at", sa.DateTime)),
        ],
    )
    source_url = f"{config.ZENDESK_CREDENTIALS['dit']['url']}/groups.json"
    email = config.ZENDESK_CREDENTIALS["dit"]["email"]
    secret = config.ZENDESK_CREDENTIALS["dit"]["secret"]
    auth_token = base64.b64encode(f"{email}/token:{secret}".encode()).decode()

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="run-fetch",
            python_callable=partial(
                fetch_from_api_endpoint,
                auth_token=self.auth_token,
                results_key='groups',
                next_key='next_page',
                auth_type="Basic",
                extra_headers={"Content-Type": "application/json"},
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
            retries=self.fetch_retries,
        )
