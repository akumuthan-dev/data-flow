import datetime

import airflow
import sqlalchemy as sa
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow.operators.db_tables import (
    create_temp_tables,
    check_table_data,
    swap_dataset_table,
    drop_temp_tables,
    insert_data_into_db,
    query_database,
)

from dataflow.dags.dataset_pipelines import (
    CompaniesDatasetPipeline,
    InvestmentProjectsDatasetPipeline,
)


class FDIDashboardPipeline:
    controller_pipeline = CompaniesDatasetPipeline
    dependencies = [InvestmentProjectsDatasetPipeline]
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
            sa.Column("foreign_equity_investment", sa.Numeric),
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
    start_date = datetime.datetime(2020, 3, 3)
    table_name = "fdi_dashboard_data"
    timeout = 7200

    @classmethod
    def get_dag(pipeline):
        # start_date = pipeline.controller_pipeline.start_date
        with airflow.DAG(
            pipeline.__name__,
            catchup=False,
            max_active_runs=1,
            start_date=pipeline.start_date,
        ) as dag:

            target_db = pipeline.controller_pipeline.target_db
            target_table = sa.Table(
                f'{pipeline.table_name}',
                sa.MetaData(),
                *[column.copy() for _, column in pipeline.field_mapping],
            )
            target_table_name = target_table.name

            _query = PythonOperator(
                task_id='query-database',
                provide_context=True,
                python_callable=query_database,
                op_args=[pipeline.query, target_db, target_table_name],
            )

            _create_tables = PythonOperator(
                task_id="create-temp-tables",
                python_callable=create_temp_tables,
                provide_context=True,
                op_args=[target_db, target_table],
            )

            _insert_into_temp_table = PythonOperator(
                task_id="insert-into-temp-table",
                python_callable=insert_data_into_db,
                provide_context=True,
                op_args=[target_db, target_table, pipeline.field_mapping],
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
                    external_task_id='drop-temp-tables',
                    timeout=pipeline.timeout,
                )
                _sensors.append(sensor)

            (
                _sensors
                >> _query
                >> _create_tables
                >> _insert_into_temp_table
                >> _check_tables
                >> _swap_dataset_table
                >> _drop_tables
            )

        return dag


pipeline = FDIDashboardPipeline
globals()[pipeline.__name__ + "__fdi_dashboard_dag"] = pipeline.get_dag()
