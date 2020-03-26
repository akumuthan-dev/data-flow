"""Airflow DAGs for Consent API"""
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hawk_api


class _ConsentPipeline(_PipelineDAG):
    cascade_drop_tables = True

    source_url: str

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-consent-api-data",
            python_callable=partial(
                fetch_from_hawk_api,
                hawk_credentials=config.CONSENT_HAWK_CREDENTIALS,
                content_type="application/json",
            ),
            provide_context=True,
            op_args=[self.table_name, self.source_url],
        )


class ConsentPipeline(_ConsentPipeline):
    table_name = "consent_dataset"
    source_url = "{}/api/v1/person/datahub_export/?limit={}".format(
        config.CONSENT_BASE_URL, config.CONSENT_RESULTS_PER_PAGE
    )

    field_mapping = [
        ("id", sa.Column("id", sa.Integer, primary_key=True)),
        ("key", sa.Column("key", sa.String)),
        ("email", sa.Column("email", sa.String)),
        ("phone", sa.Column("phone", sa.String)),
        ("key_type", sa.Column("key_type", sa.String)),
        ("created_at", sa.Column("created_at", sa.DateTime)),
        ("modified_at", sa.Column("modified_at", sa.DateTime)),
        ("current", sa.Column("current", sa.Boolean)),
        ("email_marketing_consent", sa.Column("email_marketing_consent", sa.Boolean)),
        ("phone_marketing_consent", sa.Column("phone_marketing_consent", sa.Boolean)),
    ]
