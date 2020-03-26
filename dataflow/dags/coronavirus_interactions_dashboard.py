import datetime

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.operators.db_tables import query_database

from dataflow.dags.dataset_pipelines import (
    InteractionsDatasetPipeline,
    AdvisersDatasetPipeline,
    TeamsDatasetPipeline,
    ContactsDatasetPipeline,
)
from dataflow.utils import TableConfig


class CoronavirusInteractionsDashboardPipeline(_PipelineDAG):
    dependencies = [
        InteractionsDatasetPipeline,
        AdvisersDatasetPipeline,
        TeamsDatasetPipeline,
        ContactsDatasetPipeline,
    ]

    table_config = TableConfig(
        table_name="coronavirus_interactions_dashboard_data",
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            ("interaction_date", sa.Column("interaction_date", sa.Date)),
            ("company_name", sa.Column("company_name", sa.Text)),
            ("company_country", sa.Column("company_country", sa.Text)),
            ("company_link", sa.Column("company_link", sa.Text)),
            ("company_sector", sa.Column("company_sector", sa.Text)),
            ("company_region", sa.Column("company_region", sa.Text)),
            ("subject_of_interaction", sa.Column("subject_of_interaction", sa.Text)),
            ("data_hub_link", sa.Column("data_hub_link", sa.Text)),
            ("team", sa.Column("team", sa.Text)),
            ("role", sa.Column("role", sa.Text)),
            ("policy_areas", sa.Column("policy_areas", sa.ARRAY(sa.Text))),
            ("entered_into_data_hub", sa.Column("entered_into_data_hub", sa.DateTime)),
        ],
    )

    query = '''
    with covid_interactions as (select * from interactions_dataset
    where
    (
        (interaction_subject ILIKE '%coronavirus%' or interaction_subject ILIKE '%covid%')
        or
        (interaction_notes ILIKE '%coronavirus%' or interaction_notes ILIKE '%covid%')
        or
        (policy_feedback_notes ILIKE '%coronavirus%' or policy_feedback_notes ILIKE '%covid%')
        or (
            policy_areas::text LIKE '%Coronavirus%'
        )
    )
    and interaction_date > '2019-12-05'),
    c_advisers as (
        select advisers_dataset.id as "id", teams_dataset.name as team, teams_dataset.role as role from advisers_dataset
        join teams_dataset on teams_dataset.id = advisers_dataset.team_id
        where advisers_dataset.id in (select unnest(covid_interactions.adviser_ids)::uuid from covid_interactions)
    ),
    c_contacts as (
        select contacts_dataset.id, contact_name from contacts_dataset
        where contacts_dataset.id in (select unnest(covid_interactions.contact_ids)::uuid from covid_interactions)
    )
    select
    ci.interaction_date::text as "interaction_date",
    co.name as "company_name",
    co.address_country as "company_country",
    concat('https://www.datahub.trade.gov.uk/companies/', co.id, '/activity') as "company_link",
    co.sector as "company_sector",
    co.uk_region as "company_region",
    ci.interaction_subject as "subject_of_interaction",
    ci.interaction_link as "data_hub_link",
    (select c_advisers.team from c_advisers where c_advisers.id = ci.adviser_ids[1]::uuid) as "team",
    (select c_advisers.role from c_advisers where c_advisers.id = ci.adviser_ids[1]::uuid) as "role",
    ci.policy_areas as "policy_areas",
    ci.created_on::text as "entered_into_data_hub"
    from covid_interactions ci
    join companies_dataset co on co.id = ci.company_id
    order by ci.interaction_date DESC;
    '''

    schedule_interval = '@daily'
    start_date = datetime.datetime(2020, 3, 25)
    timeout = 7200

    def get_fetch_operator(self):
        op = PythonOperator(
            task_id='query-database',
            provide_context=True,
            python_callable=query_database,
            op_args=[self.query, self.target_db, self.table_config.table_name],
        )
        return op

    def get_dag(self) -> DAG:

        dag = super().get_dag()

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
