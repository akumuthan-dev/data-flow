import datetime

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow.dags import _PipelineDAG
from dataflow.operators.db_tables import query_database

from dataflow.dags.dataset_pipelines import (
    CompaniesDatasetPipeline,
    InvestmentProjectsDatasetPipeline,
)


class FDIDashboardPipeline(_PipelineDAG):
    dependencies = [CompaniesDatasetPipeline, InvestmentProjectsDatasetPipeline]
    field_mapping = [
        ("actual_land_data", sa.Column("actual_land_date", sa.Date)),
        ("actual_uk_regions", sa.Column("actual_uk_regions", sa.ARRAY(sa.Text))),
        ("id", sa.Column("id", UUID, primary_key=True)),
        ("estimated_land_date", sa.Column("estimated_land_date", sa.Date)),
        ("investment_type", sa.Column("investment_type", sa.Text)),
        ("level_of_involvement", sa.Column("level_of_involvement", sa.Text)),
        ("number_new_jobs", sa.Column("number_new_jobs", sa.Integer)),
        ("number_safeguarded_jobs", sa.Column("number_safeguarded_jobs", sa.Integer)),
        ("possible_uk_regions", sa.Column("possible_uk_regions", sa.ARRAY(sa.Text))),
        ("fdi_value", sa.Column("fdi_value", sa.Text)),
        ("project_reference", sa.Column("project_reference", sa.Text)),
        ("investor_company_sector", sa.Column("investor_company_sector", sa.String)),
        ("stage", sa.Column("stage", sa.Text)),
        ("status", sa.Column("status", sa.Text)),
        (
            "foreign_equity_investment",
            sa.Column("foreign_equity_investment", sa.Float),
        ),
        ("address_postcode", sa.Column("address_postcode", sa.String)),
        ("investor_company_id", sa.Column("investor_company_id", UUID)),
        ("address_country", sa.Column("address_country", sa.String)),
    ]

    query = '''
    select
      investment_projects_dataset.actual_land_date,
      investment_projects_dataset.actual_uk_regions,
      investment_projects_dataset.id,
      investment_projects_dataset.estimated_land_date,
      investment_projects_dataset.investment_type,
      investment_projects_dataset.level_of_involvement,
      investment_projects_dataset.number_new_jobs,
      investment_projects_dataset.number_safeguarded_jobs,
      investment_projects_dataset.possible_uk_regions,
      investment_projects_dataset.fdi_value,
      investment_projects_dataset.project_reference,
      investment_projects_dataset.investor_company_sector,
      investment_projects_dataset.stage,
      investment_projects_dataset.status,
      investment_projects_dataset.foreign_equity_investment,
      investment_projects_dataset.address_postcode,
      investment_projects_dataset.investor_company_id,
      address_country
    from investment_projects_dataset join companies_dataset
      on companies_dataset.id = investment_projects_dataset.investor_company_id
    where investment_projects_dataset.actual_land_date between '2020-04-01' AND '2021-03-31'
     and investment_projects_dataset.estimated_land_date between '2020-04-01' AND '2021-03-31'
     and investment_projects_dataset.investment_type = 'FDI'
     and investment_projects_dataset.status = 'ongoing'
    '''

    schedule_interval = '@daily'
    start_date = datetime.datetime(2020, 3, 3)
    table_name = "fdi_dashboard_data"
    timeout = 7200

    def get_fetch_operator(self):
        op = PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[self.query, self.target_db, self.table_name],
        )
        return op

    def get_dag(self) -> DAG:

        dag = super().get_dag()
        # get fetch task for dag

        sensors = []
        for pipeline in self.dependencies:
            sensor = ExternalTaskSensor(
                task_id=f'wait_for_{pipeline.__name__.lower()}',
                external_dag_id=pipeline.__name__,
                external_task_id='drop-temp-tables',
                timeout=self.timeout,
                dag=dag,
            )
            sensors.append(sensor)
        sensors >> dag.tasks[0]
        return dag
