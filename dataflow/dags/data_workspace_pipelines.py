"""A module that defines Airflow DAGS for data workspace pipelines."""
from functools import partial

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_hawk_api
from dataflow.utils import TableConfig


class _DataWorkspacePipeline(_PipelineDAG):
    cascade_drop_tables = True
    source_url: str
    use_utc_now_as_source_modified = True

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_hawk_api,
                hawk_credentials=config.DATA_WORKSPACE_HAWK_CREDENTIALS,
                validate_response=False,
                force_http=True,  # This is a workaround until hawk auth is sorted on data workspace
            ),
            provide_context=True,
            op_args=[
                self.table_config.table_name,  # pylint: disable=no-member
                self.source_url,
            ],
            retries=self.fetch_retries,
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
            ('extra', sa.Column('extra', sa.JSON)),
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
                'application_template_name',
                sa.Column('application_template_name', sa.Text),
            ),
            ('spawner_cpu', sa.Column('spawner_cpu', sa.Text)),
            ('spawner_created_at', sa.Column('spawner_created_at', sa.DateTime)),
            ('spawner_memory', sa.Column('spawner_memory', sa.Text)),
            ('spawner_stopped_at', sa.Column('spawner_stopped_at', sa.DateTime)),
            ('state', sa.Column('state', sa.Text)),
        ],
    )


class DataWorkspaceCatalogueItemsPipeline(_DataWorkspacePipeline):
    """Pipeline meta object for data workspace catalogue items data."""

    source_url = f'{config.DATA_WORKSPACE_BASE_URL}/api/v1/dataset/catalogue-items'
    table_config = TableConfig(
        table_name='dataworkspace__catalogue_items',
        field_mapping=[
            ('purpose', sa.Column('purpose', sa.Text, nullable=False)),
            ('id', sa.Column('id', UUID, primary_key=True)),
            ('name', sa.Column('name', sa.Text, nullable=False, index=True)),
            (
                'short_description',
                sa.Column('short_description', sa.Text, nullable=False),
            ),
            ('description', sa.Column('description', sa.Text)),
            ('published', sa.Column('published', sa.Boolean, nullable=False)),
            ('created_date', sa.Column('created_date', sa.DateTime, nullable=False)),
            ('published_at', sa.Column('published_at', sa.Date, index=True)),
            (
                'information_asset_owner',
                sa.Column('information_asset_owner', sa.Integer),
            ),
            (
                'information_asset_manager',
                sa.Column('information_asset_manager', sa.Integer),
            ),
            ('enquiries_contact', sa.Column('enquiries_contact', sa.Integer)),
            ('source_tags', sa.Column('source_tags', sa.ARRAY(sa.Text))),
            ('licence', sa.Column('license', sa.Text)),
            ('personal_data', sa.Column('personal_data', sa.Text)),
            ('retention_policy', sa.Column('retention_policy', sa.Text)),
            (
                'eligibility_criteria',
                sa.Column('eligibility_criteria', sa.ARRAY(sa.Text)),
            ),
            ('slug', sa.Column('slug', sa.Text)),
            (
                'source_tables',
                TableConfig(
                    table_name='dataworkspace__source_tables',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'dataset_id': contexts[0]['id'],
                        }
                    ],
                    field_mapping=[
                        ('id', sa.Column('id', UUID, primary_key=True)),
                        ('name', sa.Column('name', sa.Text, nullable=False)),
                        ('schema', sa.Column('schema', sa.Text, nullable=False)),
                        ('table', sa.Column('table', sa.Text, nullable=False)),
                    ],
                ),
            ),
        ],
    )
