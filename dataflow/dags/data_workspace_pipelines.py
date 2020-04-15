"""A module that defines Airflow DAGS for data workspace pipelines."""
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hawk_api
from dataflow.utils import TableConfig


class _DataWorkspacePipeline(_PipelineDAG):
    cascade_drop_tables = True
    source_url: str

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_hawk_api,
                hawk_credentials=config.DATA_WORKSPACE_HAWK_CREDENTIALS,
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
        )


class DataWorkspaceEventLogPipeline(_DataWorkspacePipeline):
    """Pipeline meta object for data workspace event data."""

    source_url = f'{config.DATA_WORKSPACE_BASE_URL}/api/v1/eventlog/events'
    table_config = TableConfig(
        table_name='dataworkspace__event_log',
        field_mapping=[
            ('event_type', sa.Column('event_type', sa.Text)),
            ('id', sa.Column('id', sa.Numeric, primary_key=True)),
            (('related_object', 'id'), sa.Column('related_object_id', sa.Text)),
            (('related_object', 'name'), sa.Column('related_object_name', sa.Text)),
            (('related_object', 'type'), sa.Column('related_object_type', sa.Text)),
            ('timestamp', sa.Column('timestamp', sa.DateTime)),
            ('user_id', sa.Column('user_id', sa.Numeric)),
        ],
    )


class DataWorkspaceUserPipeline(_DataWorkspacePipeline):
    """Pipeline meta object for data workspace user data."""

    source_url = f'{config.DATA_WORKSPACE_BASE_URL}/api/v1/account/users'
    table_config = TableConfig(
        table_name='dataworkspace__users',
        field_mapping=[
            ('email', sa.Column('email', sa.Text)),
            ('first_name', sa.Column('first_name', sa.Text)),
            ('id', sa.Column('id', sa.Numeric, primary_key=True)),
            ('is_staff', sa.Column('is_staff', sa.Boolean)),
            ('is_superuser', sa.Column('is_superuser', sa.Boolean)),
            ('last_name', sa.Column('last_name', sa.Text)),
        ],
    )


class DataWorkspaceApplicationInstancePipeline(_DataWorkspacePipeline):
    """Pipeline meta object for data workspace application instance data."""

    source_url = (
        f'{config.DATA_WORKSPACE_BASE_URL}/api/v1/application-instance/instances'
    )
    table_config = TableConfig(
        table_name='dataworkspace__application_instances',
        field_mapping=[
            ('commit_id', sa.Column('commit_id', sa.Text)),
            ('cpu', sa.Column('cpu', sa.Text)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('memory', sa.Column('memory', sa.Text)),
            ('owner_id', sa.Column('owner_id', sa.Numeric)),
            ('proxy_url', sa.Column('proxy_url', sa.Text)),
            ('public_host', sa.Column('public_host', sa.Text)),
            ('spawner', sa.Column('spawner', sa.Text)),
            (
                'spawner_application_instance_id',
                sa.Column('spawner_application_instance_id', sa.Text),
            ),
            (
                'spawner_application_template_options',
                sa.Column('spawner_application_template_options', sa.Text),
            ),
            ('spawner_cpu', sa.Column('spawner_cpu', sa.Text)),
            ('spawner_created_at', sa.Column('spawner_created_at', sa.DateTime)),
            ('spawner_memory', sa.Column('spawner_memory', sa.Text)),
            ('spawner_stopped_at', sa.Column('spawner_stopped_at', sa.DateTime)),
            ('state', sa.Column('state', sa.Text)),
        ],
    )
