from datetime import datetime
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow import config
from dataflow.config import DATAHUB_HAWK_CREDENTIALS
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hawk_api
from dataflow.transforms import drop_empty_string_fields
from dataflow.utils import TableConfig


class DataHubSPIPipeline(_PipelineDAG):
    target_db = config.DATASETS_DB_NAME
    source_url = '{0}/v4/dataset/investment-projects-activity-dataset'.format(
        config.DATAHUB_BASE_URL
    )
    start_date = datetime(2020, 2, 23)
    end_date = None
    schedule_interval = '@daily'
    catchup = False
    table_config = TableConfig(
        table_name="datahub_spi",
        transforms=[drop_empty_string_fields],
        field_mapping=[
            ("aftercare_offered_on", sa.Column("aftercare_offered_on", sa.DateTime)),
            ("assigned_to_ist", sa.Column("assigned_to_ist", sa.DateTime)),
            ("enquiry_processed", sa.Column("enquiry_processed", sa.DateTime)),
            ("enquiry_processed_by_id", sa.Column("enquiry_processed_by", sa.Text)),
            ("enquiry_type", sa.Column("enquiry_type", sa.Text)),
            ("investment_project_id", sa.Column("investment_project_id", UUID)),
            (
                'project_manager_assigned',
                sa.Column('project_manager_assigned', sa.Text),
            ),
            (
                'project_manager_assigned_by_id',
                sa.Column('project_manager_assigned_by', sa.Text),
            ),
            ('project_moved_to_won', sa.Column('project_moved_to_won', sa.DateTime)),
            (
                "propositions",
                TableConfig(
                    table_name="datahub_spi_propositions",
                    transforms=[
                        drop_empty_string_fields,
                        lambda record, table_config, contexts: {
                            **record,
                            "investment_project_id": contexts[0][
                                "investment_project_id"
                            ],
                        },
                    ],
                    field_mapping=[
                        ("adviser_id", sa.Column("adviser_id", UUID)),
                        ("deadline", sa.Column("deadline", sa.Date)),
                        (
                            "investment_project_id",
                            sa.Column("investment_project_id", UUID),
                        ),
                        ("modified_on", sa.Column("modified_on", sa.DateTime)),
                        ("status", sa.Column("status", sa.Text)),
                    ],
                ),
            ),
        ],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_hawk_api, hawk_credentials=DATAHUB_HAWK_CREDENTIALS
            ),
            provide_context=True,
            op_args=[self.table_config.table.name, self.source_url],
        )
