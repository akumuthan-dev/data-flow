import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hawk_api
from dataflow.utils import TableConfig


def field_transformation(record, table_config, contexts):
    owner = record.pop("owner", None) or {}
    enquirer = record.pop("enquirer", None) or {}
    return {
        **record,
        "owner_id": owner.get("id"),
        "enquirer_id": enquirer.get("id"),
    }


class EnquiryMgmtEnquiriesPipeline(_PipelineDAG):
    schedule_interval = "@daily"
    use_utc_now_as_source_modified = True
    table_config = TableConfig(
        schema="enquiry_mgmt",
        table_name="enquiries",
        transforms=[field_transformation],
        field_mapping=[
            ("id", sa.Column("id", sa.BigInteger, primary_key=True)),
            ("owner_id", sa.Column("owner_id", sa.BigInteger, index=True)),
            ("enquirer_id", sa.Column("enquirer_id", sa.BigInteger, index=True)),
            ("created", sa.Column("created", sa.DateTime, nullable=False, index=True)),
            (
                "modified",
                sa.Column("modified", sa.DateTime, nullable=False, index=True),
            ),
            ("enquiry_stage", sa.Column("enquiry_stage", sa.String, nullable=False)),
            (
                "investment_readiness",
                sa.Column("investment_readiness", sa.String, nullable=False),
            ),
            ("quality", sa.Column("quality", sa.String, nullable=False)),
            (
                "marketing_channel",
                sa.Column("marketing_channel", sa.String, nullable=False),
            ),
            (
                "how_they_heard_dit",
                sa.Column("how_they_heard_dit", sa.String, nullable=False),
            ),
            ("primary_sector", sa.Column("primary_sector", sa.String, nullable=False)),
            ("ist_sector", sa.Column("ist_sector", sa.String, nullable=False)),
            ("country", sa.Column("country", sa.String, nullable=False)),
            ("region", sa.Column("region", sa.String, nullable=False)),
            (
                "first_response_channel",
                sa.Column("first_response_channel", sa.String, nullable=False),
            ),
            ("first_hpo_selection", sa.Column("first_hpo_selection", sa.String)),
            (
                "second_hpo_selection",
                sa.Column("second_hpo_selection", sa.String, nullable=False),
            ),
            (
                "third_hpo_selection",
                sa.Column("third_hpo_selection", sa.String, nullable=False),
            ),
            (
                "organisation_type",
                sa.Column("organisation_type", sa.String, nullable=False),
            ),
            (
                "investment_type",
                sa.Column("investment_type", sa.String, nullable=False),
            ),
            ("estimated_land_date", sa.Column("estimated_land_date", sa.DateTime)),
            (
                "new_existing_investor",
                sa.Column("new_existing_investor", sa.String, nullable=False),
            ),
            (
                "investor_involvement_level",
                sa.Column("investor_involvement_level", sa.String, nullable=False),
            ),
            (
                "specific_investment_programme",
                sa.Column("specific_investment_programme", sa.String, nullable=False),
            ),
            ("date_added_to_datahub", sa.Column("date_added_to_datahub", sa.DateTime)),
            ("project_success_date", sa.Column("project_success_date", sa.DateTime)),
            ("date_received", sa.Column("date_received", sa.DateTime)),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-enquiries",
            python_callable=fetch_from_hawk_api,
            provide_context=True,
            op_kwargs=dict(
                table_name=self.table_config.table_name,
                source_url=f"{config.ENQUIRY_MGMT_BASE_URL}/enquiries?page_size=1000&page=1",
                hawk_credentials=config.ENQUIRY_MGMT_HAWK_CREDENTIALS,
            ),
        )
