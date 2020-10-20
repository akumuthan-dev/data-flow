import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.zendesk import fetch_daily_tickets
from dataflow.utils import TableConfig

FIELD_MAPPING = [
    ("id", sa.Column("id", sa.BigInteger, primary_key=True)),
    ("external_id", sa.Column("external_id", sa.BigInteger)),
    ("via", sa.Column("via", sa.JSON)),
    ("created_at", sa.Column("created_at", sa.DateTime)),
    ("updated_at", sa.Column("updated_at", sa.DateTime)),
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
    ("collaborator_ids", sa.Column("collaborator_ids", sa.ARRAY(sa.BigInteger))),
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
]


def field_transformation(record, table_config, contexts):
    """
    Transform data export fields to match those of the Zendesk API.
    Get custom service field value.
    """

    service = next(
        field for field in record["custom_fields"] if field["id"] == 31281329
    )
    if "requester" in record:
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
            "organization_id": record["organization"]["id"]
            if record.get("organization")
            else None,
            "service": service["value"],
        }
    else:
        return {**record, "service": service["value"]}


class ZendeskUKTradeTicketsPipeline(_PipelineDAG):
    schedule_interval = "@daily"
    allow_null_columns = True
    use_utc_now_as_source_modified = True
    table_config = TableConfig(
        schema="zendesk",
        table_name="uktrade_tickets",
        transforms=[field_transformation],
        field_mapping=FIELD_MAPPING,
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
        )
