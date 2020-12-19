"""A module that defines Airflow DAGS for dun and bradstreet pipelines."""
from datetime import datetime, timedelta
from functools import partial
from typing import Optional

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.dags.base import PipelineMeta
from dataflow.operators.common import fetch_from_api_endpoint
from dataflow.operators.db_tables import (
    create_temp_tables,
    drop_temp_tables,
    insert_data_into_db,
)
from dataflow.operators.dun_and_bradstreet import update_table
from dataflow.utils import TableConfig


class _DNBPipeline(_PipelineDAG):
    cascade_drop_tables = True
    source_url: str
    use_utc_now_as_source_modified = True

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-fetch',
            python_callable=partial(
                fetch_from_api_endpoint,
                auth_token=config.DNB_AUTH_TOKEN,
                results_key='results',
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
            retries=self.fetch_retries,
        )


class DNBCompanyPipeline(_DNBPipeline):
    schedule_interval = '@once'
    allow_null_columns = True
    source_url = f'{config.DNB_BASE_URL}/api/workspace/companies/?page_size=1000'
    table_config = TableConfig(
        schema='dun_and_bradstreet',
        table_name='uk_companies',
        field_mapping=[
            ('last_updated', sa.Column('last_updated', sa.DateTime)),
            ('duns_number', sa.Column('duns_number', sa.Text, index=True)),
            (
                'global_ultimate_duns_number',
                sa.Column('global_ultimate_duns_number', sa.Text, index=True),
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
                    schema='dun_and_bradstreet',
                    table_name='uk_registration_numbers',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('duns_number', sa.Column('duns_number', sa.Text, index=True)),
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
                    schema='dun_and_bradstreet',
                    table_name='uk_industry_codes',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('code', sa.Column('code', sa.Text)),
                        ('description', sa.Column('description', sa.Text)),
                        ('duns_number', sa.Column('duns_number', sa.Text, index=True)),
                        ('priority', sa.Column('priority', sa.Numeric)),
                        ('typeDescription', sa.Column('typeDescription', sa.Text)),
                        ('typeDnBCode', sa.Column('typeDnBCode', sa.Text)),
                    ],
                ),
            ),
            (
                'primary_industry_codes',
                TableConfig(
                    schema='dun_and_bradstreet',
                    table_name='uk_primary_industry_codes',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('duns_number', sa.Column('duns_number', sa.Text, index=True)),
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


class DNBGlobalCompanyUpdatePipeline(metaclass=PipelineMeta):
    target_db: str = config.DATASETS_DB_NAME
    start_date: datetime = datetime(2020, 8, 13)
    end_date: Optional[datetime] = None
    schedule_interval: str = '@daily'
    catchup: bool = True
    source_url = (
        str(config.DNB_BASE_URL)
        + '/api/companies/?page_size=1000&last_updated_after={{ ds }}'
    )
    table_config = TableConfig(
        schema='dun_and_bradstreet',
        table_name='global_companies',
        field_mapping=[
            ('last_updated', sa.Column('last_updated', sa.DateTime)),
            ('duns_number', sa.Column('duns_number', sa.Text, index=True)),
            (
                'global_ultimate_duns_number',
                sa.Column('global_ultimate_duns_number', sa.Text, index=True),
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
                    schema='dun_and_bradstreet',
                    table_name='global_company_registration_numbers',
                    transforms=[
                        lambda record, table_config, contexts: {
                            **record,
                            'duns_number': contexts[0]['duns_number'],
                        }
                    ],
                    field_mapping=[
                        ('duns_number', sa.Column('duns_number', sa.Text, index=True)),
                        ('registration_type', sa.Column('registration_type', sa.Text)),
                        (
                            'registration_number',
                            sa.Column('registration_number', sa.Text),
                        ),
                    ],
                ),
            ),
        ],
    )
    companies_update_query = '''
        UPDATE {schema}.{target_table}
        SET
            last_updated=tmp.last_updated,
            primary_name=tmp.primary_name,
            trading_names=tmp.trading_names,
            address_line_1=tmp.address_line_1,
            address_line_2=tmp.address_line_2,
            address_town=tmp.address_town,
            address_county=tmp.address_county,
            address_postcode=tmp.address_postcode,
            registered_address_line_1=tmp.registered_address_line_1,
            registered_address_line_2=tmp.registered_address_line_2,
            registered_address_town=tmp.registered_address_town,
            registered_address_county=tmp.registered_address_county,
            registered_address_postcode=tmp.registered_address_postcode,
            line_of_business=tmp.line_of_business,
            is_out_of_business=tmp.is_out_of_business,
            year_started=tmp.year_started,
            global_ultimate_duns_number=tmp.global_ultimate_duns_number,
            employee_number=tmp.employee_number,
            is_employees_number_estimated=tmp.is_employees_number_estimated,
            annual_sales=tmp.annual_sales,
            is_annual_sales_estimated=tmp.is_annual_sales_estimated,
            legal_status=tmp.legal_status,
            domain=tmp.domain,
            global_ultimate_primary_name=tmp.global_ultimate_primary_name,
            annual_sales_currency=tmp.annual_sales_currency
        FROM {schema}.{from_table} tmp
        WHERE {target_table}.duns_number = tmp.duns_number;
    '''
    registration_numbers_update_query = '''
        UPDATE {schema}.{target_table}
        SET
            registration_type=tmp.registration_type,
            registration_number=tmp.registration_number
        FROM {schema}.{from_table} tmp
        JOIN dun_and_bradstreet.global_companies gc on gc.duns_number = tmp.duns_number
        WHERE gc.id = {target_table}.company_id;
    '''

    def get_dag(self):
        dag = DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': self.start_date,
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
        )

        _create_temp_tables = PythonOperator(
            task_id='create-temp-tables',
            python_callable=create_temp_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
            dag=dag,
        )

        _fetch = PythonOperator(
            task_id='run-fetch',
            dag=dag,
            python_callable=partial(
                fetch_from_api_endpoint,
                auth_token=config.DNB_AUTH_TOKEN,
                results_key='results',
            ),
            provide_context=True,
            op_args=[self.table_config.table_name, self.source_url],
        )

        _insert_into_temp_tables = PythonOperator(
            task_id="insert-into-temp-table",
            dag=dag,
            python_callable=insert_data_into_db,
            provide_context=True,
            op_kwargs=(dict(target_db=self.target_db, table_config=self.table_config)),
        )

        _update_companies = PythonOperator(
            task_id='run-update-companies',
            dag=dag,
            python_callable=update_table,
            provide_context=True,
            op_args=[
                self.target_db,
                self.table_config.tables[0],
                self.companies_update_query,
            ],
        )

        _update_reg_numbers = PythonOperator(
            task_id='run-update-reg-numbers',
            dag=dag,
            python_callable=update_table,
            provide_context=True,
            op_args=[
                self.target_db,
                self.table_config.tables[1],
                self.registration_numbers_update_query,
            ],
        )

        _drop_temp_tables = PythonOperator(
            task_id='drop-temp-tables',
            dag=dag,
            trigger_rule='all_success',
            python_callable=drop_temp_tables,
            execution_timeout=timedelta(minutes=10),
            provide_context=True,
            op_args=[self.target_db, *self.table_config.tables],
        )

        [_fetch, _create_temp_tables] >> _insert_into_temp_tables >> [
            _update_companies,
            _update_reg_numbers,
        ] >> _drop_temp_tables
        return dag
