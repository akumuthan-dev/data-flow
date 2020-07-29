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
from dataflow.dags.companies_house_pipelines import CompaniesHouseCompaniesPipeline
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

# # My addtions # TODO for non eu only? where is eu? pipeline?
# class HMRCMatchingPipeline(_CompanyMatchingPipeline):
#     # HMRCDatasetPipeline nothing existis. THere is a mapping seperately in hmrc_pipelines.py
#     controller_pipeline = None # HMRCDatasetPipeline. onlu non eu imports and exports? where is EU data?
#     dependencies = [controller_pipeline]
#     company_match_query = f"""
#         SELECT distinct on (id)
#             id as id,
#             company_name as company_name,
#             customer_email_address as contact_email,
#             NULLIF(cdms_reference, '') AS cdms_ref,
#             null as postcode,
#             null as copmanies_house_id,
#             'dit.export-wins' as source,
#             created::timestamp as datetime
#         FROM {None.table_config.table_name}
#         ORDER BY id asc, created::timestamp desc
#     """

class CompaniesHouseMatchingPipeline(_CompanyMatchingPipeline):
    controller_pipeline = CompaniesHouseCompaniesPipeline # looks correct although no L1
    dependencies = [CompaniesHouseCompaniesPipeline]
    company_match_query = f"""
        SELECT distinct on (id)
            id as id,
            company_name as company_name,
            null as contact_email,
            null as cdms_ref,
            regaddress.pobox as postcode,
            companies_house_id as copmanies_house_id,
            'companies_house.companies.L1' as source,  
            last_made_up_date as datetime
        FROM {CompaniesHouseCompaniesPipeline.table_config.table_name}
        ORDER BY id asc,  desc
    """
    # IS this fomat correct is L1 correct given CompaniesHouseCompaniesPipeline definition
    # what is cdms_ref- why is it so important.
    # IS companies_house id  SUPPOSED TO BE SPELT WRONG ??
    # Why do we care about record timestamp for matching? Need to check usage to see what else may be useful
    # Going with accoun last made up for now but seems weird to === apples and oranges.
    # id order and timestamp order - so seems most recent!
    # last_made_up_date vs return_last_made_up_date. TODO check this
