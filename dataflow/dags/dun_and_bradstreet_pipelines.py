"""A module that defines Airflow DAGS for dun and bradstreet pipelines."""
from functools import partial

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.operators.common import fetch_from_token_authenticated_api
from dataflow.utils import TableConfig


class _DNBPipeline(_PipelineDAG):
    cascade_drop_tables = True
    source_url: str

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_token_authenticated_api, token=config.DNB_AUTH_TOKEN
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
        )


class DNBCompanyPipeline(_DNBPipeline):
    schedule_interval = '@once'
    allow_null_columns = True
    source_url = f'{config.DNB_BASE_URL}/api/workspace/companies/?page_size=1000'
    table_config = TableConfig(
        table_name='dnb__companies',
        field_mapping=[
            ('last_updated', sa.Column('last_updated', sa.DateTime)),
            ('duns_number', sa.Column('duns_number', sa.Text)),
            (
                'global_ultimate_duns_number',
                sa.Column('global_ultimate_duns_number', sa.Text),
            ),
            ('primary_name', sa.Column('primary_name', sa.Text)),
            (
                'global_ultimate_primary_name',
                sa.Column('global_ultimate_primary_name', sa.Text),
            ),
            ('trading_names', sa.Column('trading_names', sa.ARRAY(sa.String))),
            ('domain', sa.Column('domain', sa.Text)),
            ('address_line_1', sa.Column('address_line_1', sa.Text)),
            ('address_line_2', sa.Column('address_line_2', sa.Text)),
            ('address_town', sa.Column('address_town', sa.Text)),
            ('address_county', sa.Column('address_county', sa.Text)),
            ('address_country', sa.Column('address_country', sa.Text)),
            ('address_postcode', sa.Column('address_postcode', sa.Text)),
            (
                'registered_address_line_1',
                sa.Column('registered_address_line_1', sa.Text),
            ),
            (
                'registered_address_line_2',
                sa.Column('registered_address_line_2', sa.Text),
            ),
            ('registered_address_town', sa.Column('registered_address_town', sa.Text)),
            (
                'registered_address_county',
                sa.Column('registered_address_county', sa.Text),
            ),
            (
                'registered_address_country',
                sa.Column('registered_address_country', sa.Text),
            ),
            (
                'registered_address_postcode',
                sa.Column('registered_address_postcode', sa.Text),
            ),
            ('line_of_business', sa.Column('line_of_business', sa.Text)),
            ('is_out_of_business', sa.Column('is_out_of_business', sa.Boolean)),
            ('year_started', sa.Column('year_started', sa.Numeric)),
            ('employee_number', sa.Column('employee_number', sa.Numeric)),
            (
                'is_employees_number_estimated',
                sa.Column('is_employees_number_estimated', sa.Boolean),
            ),
            ('annual_sales', sa.Column('annual_sales', sa.Float)),
            ('annual_sales_currency', sa.Column('annual_sales_currency', sa.Text)),
            (
                'is_annual_sales_estimated',
                sa.Column('is_annual_sales_estimated', sa.Boolean),
            ),
            ('legal_status', sa.Column('legal_status', sa.Text)),
            (
                'registration_numbers',
                TableConfig(
                    table_name='dnb__registration_numbers',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('registration_type', sa.Column('registration_type', sa.Text)),
                        (
                            'registration_number',
                            sa.Column('registration_number', sa.Text),
                        ),
                    ],
                ),
            ),
            (
                'industry_codes',
                TableConfig(
                    table_name='dnb__industry_codes',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('code', sa.Column('code', sa.Text)),
                        ('description', sa.Column('description', sa.Text)),
                        ('priority', sa.Column('priority', sa.Numeric)),
                        ('typeDescription', sa.Column('typeDescription', sa.Text)),
                        ('typeDnBCode', sa.Column('typeDnBCode', sa.Text)),
                    ],
                ),
            ),
            (
                'primary_industry_codes',
                TableConfig(
                    table_name='dnb__primary_industry_codes',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('usSicV4', sa.Column('usSicV4', sa.Text)),
                        (
                            'usSicV4Description',
                            sa.Column('usSicV4Description', sa.Text),
                        ),
                    ],
                ),
            ),
            ('worldbase_source', sa.Column('worldbase_source', sa.JSON)),
            ('source', sa.Column('source', sa.Text)),
        ],
    )
