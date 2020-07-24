from typing import Type

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.dags import _PipelineDAG
from dataflow.dags.activity_stream_pipelines import (
    GreatGOVUKExportOpportunityEnquiriesPipeline,
)
from dataflow.dags.companies_house_pipelines import CompaniesHouseCompaniesPipeline
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
            schema=self.controller_pipeline.table_config.schema,
            table_name=self.table_name,
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

    @property
    def table_name(self):
        return f'{self.controller_pipeline.table_config.table_name}_match_ids'

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
        FROM {CompaniesDatasetPipeline.fq_table_name()} companies
        LEFT JOIN {ContactsDatasetPipeline.fq_table_name()} contacts
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
            null as companies_house_id,
            'dit.export-wins' as source,
            created::timestamp as datetime
        FROM {ExportWinsWinsDatasetPipeline.fq_table_name()}
        ORDER BY id asc, created::timestamp desc
    """


class GreatGOVUKExportOpportunityEnquiriesMatchingPipeline(_CompanyMatchingPipeline):
    table_name = 'great_gov_uk_ex_opps_enquiries_match_ids'
    controller_pipeline = GreatGOVUKExportOpportunityEnquiriesPipeline
    dependencies = [GreatGOVUKExportOpportunityEnquiriesPipeline]
    company_match_query = f"""
        SELECT distinct on (id)
            id as id,
            company_name as company_name,
            contact_email_address as contact_email,
            null AS cdms_ref,
            upper(replace(trim("postcode"), ' ', '')) as postcode,
            company_number as companies_house_id,
            'dit.export-opps-enquiries' as source,
            published as datetime
        FROM {GreatGOVUKExportOpportunityEnquiriesPipeline.fq_table_name()}
        ORDER BY id asc, published desc
    """


class CompaniesHouseMatchingPipeline(_CompanyMatchingPipeline):
    controller_pipeline = CompaniesHouseCompaniesPipeline
    dependencies = [CompaniesHouseCompaniesPipeline]
    company_match_query = f"""
        SELECT distinct on (id)
            id as id,
            company_name as company_name,
            null as contact_email,
            null as cdms_ref,
            upper(replace(trim("postcode"), ' ', '')) as postcode,
            company_number as companies_house_id,
            'companies_house.companies' as source,
            publish_date::timestamp as datetime
        FROM {CompaniesHouseCompaniesPipeline.fq_table_name()}
        ORDER BY id asc, publish_date::timestamp desc
    """
