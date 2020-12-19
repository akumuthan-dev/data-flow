"""A module that defines Airflow DAGS for gateway to research pipelines."""
from datetime import datetime

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow.dags import _PipelineDAG
from dataflow.operators.gateway_to_research import fetch_from_gtr_api
from dataflow.utils import TableConfig


class _GatewayToResearchPipeline(_PipelineDAG):
    resource_type: str

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=fetch_from_gtr_api,
            provide_context=True,
            op_args=[self.table_config.table_name, self.resource_type],
            retries=self.fetch_retries,
        )


class GatewayToResearchOrganisationsPipeline(_GatewayToResearchPipeline):
    """Pipeline meta object for GTR organisations data."""

    allow_null_columns = True
    resource_type = 'organisation'
    table_config = TableConfig(
        schema='ukri',
        table_name='gatewaytoresearch__organisations',
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'created': datetime.utcfromtimestamp(record['created'] / 1000),
            }
        ],
        field_mapping=[
            ('id', sa.Column('id', UUID(as_uuid=True), primary_key=True)),
            ('created', sa.Column('created_date', sa.DateTime)),
            ('name', sa.Column('name', sa.Text, nullable=False, index=True)),
            ('regNumber', sa.Column('registration_number', sa.Text, index=True)),
            ('website', sa.Column('website', sa.Text)),
            (
                ('addresses', 'address'),
                TableConfig(
                    schema='ukri',
                    table_name='gatewaytoresearch__organisation_addresses',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'organisation_id': contexts[0]['id'],
                        }
                    ],
                    field_mapping=[
                        (
                            'organisation_id',
                            sa.Column(
                                'organisation_id', UUID(as_uuid=True), index=True
                            ),
                        ),
                        ('line1', sa.Column('line_1', sa.Text)),
                        ('line2', sa.Column('line_2', sa.Text)),
                        ('line3', sa.Column('line_3', sa.Text)),
                        ('line4', sa.Column('line_4', sa.Text)),
                        ('line5', sa.Column('line_5', sa.Text)),
                        ('city', sa.Column('city', sa.Text)),
                        ('county', sa.Column('county', sa.Text)),
                        ('postCode', sa.Column('post_code', sa.Text)),
                        ('region', sa.Column('region', sa.Text)),
                        ('country', sa.Column('country', sa.Text)),
                        ('type', sa.Column('type', sa.Text, nullable=False)),
                    ],
                ),
            ),
            (
                ('links', 'link'),
                TableConfig(
                    schema='ukri',
                    table_name='gatewaytoresearch__organisation_links',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'organisation_id': contexts[0]['id'],
                            'link_id': record['href'][-36:]
                            if record['href'].startswith(
                                'https://gtr.ukri.org:443/gtr/api'
                            )
                            else '00000000-0000-0000-0000-000000000000',
                        }
                    ],
                    field_mapping=[
                        (
                            'organisation_id',
                            sa.Column(
                                'organisation_id', UUID(as_uuid=True), index=True
                            ),
                        ),
                        ('rel', sa.Column('type', sa.Text, index=True)),
                        (
                            'link_id',
                            sa.Column('link_id', UUID(as_uuid=True), index=True),
                        ),
                    ],
                ),
            ),
        ],
    )


class GatewayToResearchPersonsPipeline(_GatewayToResearchPipeline):
    """Pipeline meta object for GTR persons data."""

    allow_null_columns = True
    resource_type = 'person'
    table_config = TableConfig(
        schema='ukri',
        table_name='gatewaytoresearch__persons',
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'created': datetime.utcfromtimestamp(record['created'] / 1000),
            }
        ],
        field_mapping=[
            ('id', sa.Column('id', UUID(as_uuid=True), primary_key=True)),
            ('created', sa.Column('created_date', sa.DateTime)),
            ('firstName', sa.Column('first_name', sa.Text, nullable=False)),
            ('otherNames', sa.Column('other_names', sa.Text)),
            ('surname', sa.Column('surname', sa.Text, nullable=False, index=True)),
            ('email', sa.Column('email', sa.Text, index=True)),
            (
                ('links', 'link'),
                TableConfig(
                    schema='ukri',
                    table_name='gatewaytoresearch__person_links',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'person_id': contexts[0]['id'],
                            'link_id': record['href'][-36:]
                            if record['href'].startswith(
                                'https://gtr.ukri.org:443/gtr/api'
                            )
                            else '00000000-0000-0000-0000-000000000000',
                        }
                    ],
                    field_mapping=[
                        (
                            'person_id',
                            sa.Column('person_id', UUID(as_uuid=True), index=True),
                        ),
                        ('rel', sa.Column('type', sa.Text, index=True)),
                        (
                            'link_id',
                            sa.Column('link_id', UUID(as_uuid=True), index=True),
                        ),
                    ],
                ),
            ),
        ],
    )


class GatewayToResearchProjectsPipeline(_GatewayToResearchPipeline):
    """Pipeline meta object for GTR projects data."""

    allow_null_columns = True
    resource_type = 'project'
    table_config = TableConfig(
        schema='ukri',
        table_name='gatewaytoresearch__projects',
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'created': datetime.utcfromtimestamp(record['created'] / 1000),
                'normalisedHealthCategories': [
                    r['text'] for r in record['healthCategories']['healthCategory']
                ],
                'normalisedResearchActivities': [
                    r['text'] for r in record['researchActivities']['researchActivity']
                ],
                'normalisedResearchSubjects': [
                    r['text'] for r in record['researchSubjects']['researchSubject']
                ],
                'normalisedResearchTopics': [
                    r['text'] for r in record['researchTopics']['researchTopic']
                ],
                'normalisedRcukProgrammes': [
                    r['text'] for r in record['rcukProgrammes']['rcukProgramme']
                ],
                'start': datetime.utcfromtimestamp(record['start'] / 1000)
                if record['start']
                else None,
                'end': datetime.utcfromtimestamp(record['end'] / 1000)
                if record['end']
                else None,
            }
        ],
        field_mapping=[
            ('id', sa.Column('id', UUID(as_uuid=True), primary_key=True)),
            ('created', sa.Column('created_date', sa.DateTime)),
            ('title', sa.Column('title', sa.Text, nullable=False, index=True)),
            ('status', sa.Column('status', sa.Text)),
            ('grantCategory', sa.Column('grant_category', sa.Text)),
            ('leadFunder', sa.Column('lead_funder', sa.Text)),
            (
                'leadOrganisationDepartment',
                sa.Column('lead_organisation_department', sa.Text),
            ),
            ('abstractText', sa.Column('abstract', sa.Text)),
            ('techAbstractText', sa.Column('tech_abstract', sa.Text)),
            ('potentialImpact', sa.Column('potential_impact', sa.Text)),
            (
                'normalisedHealthCategories',
                sa.Column('health_categories', sa.ARRAY(sa.Text)),
            ),
            (
                'normalisedResearchActivities',
                sa.Column('research_activities', sa.ARRAY(sa.Text)),
            ),
            (
                'normalisedResearchSubjects',
                sa.Column('research_subjects', sa.ARRAY(sa.Text)),
            ),
            (
                'normalisedResearchTopics',
                sa.Column('research_topics', sa.ARRAY(sa.Text)),
            ),
            (
                'normalisedRcukProgrammes',
                sa.Column('rcuk_programmes', sa.ARRAY(sa.Text)),
            ),
            ('start', sa.Column('start_date', sa.DateTime)),
            ('end', sa.Column('end_date', sa.DateTime)),
            (
                ('identifiers', 'identifier'),
                TableConfig(
                    schema='ukri',
                    table_name='gatewaytoresearch__project_identifiers',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'project_id': contexts[0]['id'],
                        }
                    ],
                    field_mapping=[
                        (
                            'project_id',
                            sa.Column('project_id', UUID(as_uuid=True), index=True),
                        ),
                        ('value', sa.Column('value', sa.Text)),
                        ('type', sa.Column('type', sa.Text, index=True)),
                    ],
                ),
            ),
            (
                ('links', 'link'),
                TableConfig(
                    schema='ukri',
                    table_name='gatewaytoresearch__project_links',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'project_id': contexts[0]['id'],
                            'link_id': record['href'][-36:]
                            if record['href'].startswith(
                                'https://gtr.ukri.org:443/gtr/api'
                            )
                            else '00000000-0000-0000-0000-000000000000',
                        }
                    ],
                    field_mapping=[
                        (
                            'project_id',
                            sa.Column('project_id', UUID(as_uuid=True), index=True),
                        ),
                        ('rel', sa.Column('type', sa.Text, index=True)),
                        (
                            'link_id',
                            sa.Column('link_id', UUID(as_uuid=True), index=True),
                        ),
                    ],
                ),
            ),
        ],
    )


class GatewayToResearchFundsPipeline(_GatewayToResearchPipeline):
    """Pipeline meta object for GTR funds data."""

    allow_null_columns = True
    resource_type = 'fund'
    table_config = TableConfig(
        schema='ukri',
        table_name='gatewaytoresearch__funds',
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                'created': datetime.utcfromtimestamp(record['created'] / 1000),
                'start': datetime.utcfromtimestamp(record['start'] / 1000)
                if record['start']
                else None,
                'end': datetime.utcfromtimestamp(record['end'] / 1000)
                if record['end']
                else None,
                'valuePounds': record['valuePounds']['amount'],
            }
        ],
        field_mapping=[
            ('id', sa.Column('id', UUID(as_uuid=True), primary_key=True)),
            ('created', sa.Column('created_date', sa.DateTime)),
            ('start', sa.Column('start', sa.Text)),
            ('end', sa.Column('end', sa.Text)),
            ('valuePounds', sa.Column('value', sa.Numeric)),
            ('category', sa.Column('category', sa.Text)),
            ('type', sa.Column('type', sa.Text, index=True)),
            (
                ('links', 'link'),
                TableConfig(
                    schema='ukri',
                    table_name='gatewaytoresearch__fund_links',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'fund_id': contexts[0]['id'],
                            'link_id': record['href'][-36:]
                            if record['href'].startswith(
                                'https://gtr.ukri.org:443/gtr/api'
                            )
                            else '00000000-0000-0000-0000-000000000000',
                        }
                    ],
                    field_mapping=[
                        (
                            'fund_id',
                            sa.Column('fund_id', UUID(as_uuid=True), index=True),
                        ),
                        ('rel', sa.Column('type', sa.Text, index=True)),
                        (
                            'link_id',
                            sa.Column('link_id', UUID(as_uuid=True), index=True),
                        ),
                    ],
                ),
            ),
        ],
    )
