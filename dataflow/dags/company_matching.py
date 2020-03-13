from typing import List, Type

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.dags.dataset_pipelines import (
    ContactsDatasetPipeline,
    CompaniesDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
)
from dataflow.operators.company_matching import fetch_from_company_matching


class _CompanyMatchingPipeline(_PipelineDAG):
    timeout: int = 7200
    field_mapping = [
        ("id", sa.Column("id", sa.Text, primary_key=True)),
        ("match_id", sa.Column("match_id", sa.Integer)),
        ("similarity", sa.Column("similarity", sa.Text)),
    ]

    company_match_query: str
    controller_pipeline: Type[_PipelineDAG]
    dependencies: List[Type[_PipelineDAG]] = []

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id=f'company-match-data',
            python_callable=fetch_from_company_matching,
            provide_context=True,
            op_args=[
                self.target_db,
                self.table_name,
                self.company_match_query,
                config.MATCHING_SERVICE_BATCH_SIZE,
            ],
        )

    def get_dag(self) -> DAG:
        self.table_name = f'{self.controller_pipeline.table_name}_match_ids'
        self.start_date = self.controller_pipeline.start_date
        self.schedule_interval = self.controller_pipeline.schedule_interval

        dag = super().get_dag()

        for pipeline in [self.controller_pipeline] + self.dependencies:
            sensor = ExternalTaskSensor(
                task_id=f'wait_for_{pipeline.__name__.lower()}',
                external_dag_id=pipeline.__name__,
                external_task_id='drop-temp-tables',
                timeout=self.timeout,
                dag=dag,
            )

            dag.set_dependency(sensor.task_id, "company-match-data")

        return dag


class DataHubMatchingPipeline(_CompanyMatchingPipeline):
    controller_pipeline = CompaniesDatasetPipeline
    dependencies = [ContactsDatasetPipeline]
    company_match_query = f"""
        SELECT distinct on (companies.id)
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
        ORDER BY companies.id asc, companies.modified_on desc
    """


class ExportWinsMatchingPipeline(_CompanyMatchingPipeline):
    controller_pipeline = ExportWinsWinsDatasetPipeline
    company_match_query = f"""
        SELECT distinct on (id)
            id as id,
            company_name as company_name,
            customer_email_address as contact_email,
            NULLIF(cdms_reference, '') AS cdms_ref,
            null as postcode,
            null as copmanies_house_id,
            'dit.export-wins' as source,
            created::timestamp as datetime
        FROM {ExportWinsWinsDatasetPipeline.table_name}
        ORDER BY id asc, created::timestamp desc
    """