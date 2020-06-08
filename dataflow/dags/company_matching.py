from typing import Type

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.dags.dataset_pipelines import (
    ContactsDatasetPipeline,
    CompaniesDatasetPipeline,
    ExportWinsWinsDatasetPipeline,
)
from dataflow.operators.company_matching import fetch_from_company_matching
from dataflow.utils import TableConfig


class _CompanyMatchingPipeline(_PipelineDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table_config = TableConfig(
            table_name=f'{self.controller_pipeline.table_config.table_name}_match_ids',
            field_mapping=[
                ("id", sa.Column("id", sa.Text, primary_key=True)),
                ("match_id", sa.Column("match_id", sa.Integer)),
                ("similarity", sa.Column("similarity", sa.Text)),
            ],
        )

        self.start_date = self.controller_pipeline.start_date
        self.schedule_interval = self.controller_pipeline.schedule_interval

    company_match_query: str
    controller_pipeline: Type[_PipelineDAG]

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='company-match-data',
            python_callable=fetch_from_company_matching,
            provide_context=True,
            op_args=[
                self.target_db,
                self.table_config.table_name,
                self.company_match_query,
                config.MATCHING_SERVICE_BATCH_SIZE,
            ],
        )


class DataHubMatchingPipeline(_CompanyMatchingPipeline):
    controller_pipeline = CompaniesDatasetPipeline
    dependencies = [CompaniesDatasetPipeline, ContactsDatasetPipeline]
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
        FROM {CompaniesDatasetPipeline.table_config.table_name} companies
        LEFT JOIN {ContactsDatasetPipeline.table_config.table_name} contacts
        ON contacts.company_id = companies.id
        ORDER BY companies.id asc, companies.modified_on desc
    """


class ExportWinsMatchingPipeline(_CompanyMatchingPipeline):
    controller_pipeline = ExportWinsWinsDatasetPipeline
    dependencies = [ExportWinsWinsDatasetPipeline]
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
        FROM {ExportWinsWinsDatasetPipeline.table_config.table_name}
        ORDER BY id asc, created::timestamp desc
    """
