from datetime import timedelta
from typing import List

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from dataflow import config
from dataflow.dags.dataset_pipelines import (
    ContactsDatasetPipeline,
    CompaniesDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
    BaseDatasetPipeline,
)
from dataflow.operators.company_matching import fetch_from_company_matching
from dataflow.operators.db_tables import (
    create_temp_tables,
    check_table_data,
    swap_dataset_table,
    drop_temp_tables,
)


class BaseCompanyMatchingPipeline:
    timeout = 7200

    @classmethod
    def get_dag(pipeline):
        target_db = pipeline.controller_pipeline.target_db
        target_table = sa.Table(
            f'{pipeline.controller_pipeline.table_name}_match_ids',
            sa.MetaData(),
            sa.Column("id", sa.Text, primary_key=True),
            sa.Column("match_id", sa.Integer),
            sa.Column("similarity", sa.Text),
        )

        with DAG(
            dag_id=pipeline.__name__,
            catchup=False,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'priority_weight': 1,
            },
            max_active_runs=1,
            start_date=pipeline.controller_pipeline.start_date,
            schedule_interval=pipeline.controller_pipeline.schedule_interval,
        ) as target_dag:

            _create_tables = PythonOperator(
                task_id="create-temp-tables",
                python_callable=create_temp_tables,
                provide_context=True,
                op_args=[target_db, target_table],
            )

            _fetch = PythonOperator(
                task_id=f'{pipeline.controller_pipeline.__name__.lower()}-matching-task',
                python_callable=fetch_from_company_matching,
                provide_context=True,
                op_kwargs={
                    'target_db': pipeline.controller_pipeline.target_db,
                    'table': target_table,
                    'company_match_query': pipeline.company_match_query,
                    'batch_size': config.MATCHING_SERVICE_BATCH_SIZE,
                },
            )

            _check_tables = PythonOperator(
                task_id="check-temp-table-data",
                python_callable=check_table_data,
                provide_context=True,
                op_args=[target_db, target_table],
            )

            _swap_dataset_table = PythonOperator(
                task_id="swap-dataset-table",
                python_callable=swap_dataset_table,
                provide_context=True,
                op_args=[target_db, target_table],
            )

            _drop_tables = PythonOperator(
                task_id="drop-temp-tables",
                python_callable=drop_temp_tables,
                provide_context=True,
                trigger_rule="all_done",
                op_args=[target_db, target_table],
            )

            _sensors = []
            for _pipeline in [pipeline.controller_pipeline] + pipeline.dependencies:
                sensor = ExternalTaskSensor(
                    task_id=f'wait_for_{_pipeline.__name__.lower()}',
                    external_dag_id=_pipeline.__name__,
                    external_task_id='insert-from-temporary-table',
                    timeout=pipeline.timeout,
                )
                _sensors.append(sensor)

        (
            _sensors
            >> _create_tables
            >> _fetch
            >> _check_tables
            >> _swap_dataset_table
            >> _drop_tables
        )
        return target_dag


class DataHubMatchingPipeline(BaseCompanyMatchingPipeline):
    controller_pipeline = CompaniesDatasetPipeline
    dependencies = [ContactsDatasetPipeline]
    company_match_query = f"""
        SELECT distinct
            companies.id as id,
            companies.name as company_name,
            contacts.email as contact_email,
            NULLIF(companies.cdms_reference_code, '') as cdms_ref,
            companies.address_postcode as postcode,
            companies.company_number as companies_house_id,
            'dit.datahub' as source,
            companies.modified_on as datetime
        FROM {CompaniesDatasetPipeline.table_name} companies
        LEFT JOIN {ContactsDatasetPipeline.table_name} contacts
        ON contacts.company_id = companies.id
    """


class ExportWinsMatchingPipeline(BaseCompanyMatchingPipeline):
    controller_pipeline = ExportWinsWinsDatasetPipeline
    dependencies: List[BaseDatasetPipeline] = []
    company_match_query = f"""
        SELECT distinct
            id as id,
            company_name as company_name,
            customer_email_address as contact_email,
            NULLIF(cdms_reference, '') AS cdms_ref,
            null as postcode,
            null as copmanies_house_id,
            'dit.export-wins' as source,
            created::timestamp as datetime
        FROM {ExportWinsWinsDatasetPipeline.table_name}
    """


for pipeline in BaseCompanyMatchingPipeline.__subclasses__():
    globals()[pipeline.__name__ + "__matching_dag"] = pipeline.get_dag()
