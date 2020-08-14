# pip install tensorflow==2.1.0
# pip install sklearn
# pip install memory_profiler

from airflow.operators.sensors import ExternalTaskSensor

from dataflow.operators.tags_classifier_train.tags_classifier_train import fetch_interaction_labelled_data, build_models_pipeline

from dataflow.operators.db_tables import (
    check_table_data,
    create_temp_tables,
    drop_swap_tables,
    drop_temp_tables,
    insert_data_into_db,
    swap_dataset_tables,
)

from functools import partial


from datetime import timedelta
from dataflow import config
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from dataflow.utils import TableConfig, slack_alert
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
from dataflow.dags.dataset_pipelines import InteractionsDatasetPipeline

# from dataflow.dags import _ModelPipeline


import datetime


class _TrainModelPipeline(_PipelineDAG):

    controller_pipeline: Type[_PipelineDAG]
    query: str


    def fetch_data_operator(self) -> PythonOperator:
        raise NotImplementedError(
            f"{self.__class__} needs to override fetch_data_operator"
        )

    def build_model_operator(self) -> PythonOperator:
        raise NotImplementedError(
            f"{self.__class__} needs to override prediction_operator"
        )
    #
    # def measure_model_operator(self) -> PythonOperator:
    #     raise NotImplementedError(
    #         f"{self.__class__} needs to override prediction_operator"
    #     )
    #
    # def save_model_operator(self) -> PythonOperator:
    #     raise NotImplementedError(
    #         f"{self.__class__} needs to override write_prediction_operator"
    #     )

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
                "retry_delay": timedelta(minutes=5),
                'catchup': self.catchup,
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
            max_active_runs=1,
            on_success_callback=partial(slack_alert, success=True)
            if self.alert_on_success
            else None,
            on_failure_callback=partial(slack_alert, success=False)
            if self.alert_on_failure
            else None,
        )


        _fetch_data = self.fetch_data_operator()
        _fetch_data.dag = dag

        _build_model = self.build_model_operator()
        _build_model.dag = dag
        #
        # _measure_model = self.measure_model_operator()
        # _measure_model.dag = dag
        #
        # _create_tables = PythonOperator(
        #     task_id="create-temp-tables",
        #     python_callable=create_temp_tables,
        #     execution_timeout=timedelta(minutes=10),
        #     provide_context=True,
        #     op_args=[self.target_db, *self.table_config.tables],
        #     dag=dag,
        # )
        #
        # _insert_into_temp_table = PythonOperator(
        #     task_id="insert-into-temp-table",
        #     python_callable=self.get_insert_data_callable(),
        #     provide_context=True,
        #     op_kwargs=(dict(target_db=self.target_db, table_config=self.table_config)),
        # )
        #
        # _check_tables = PythonOperator(
        #     task_id="check-temp-table-data",
        #     python_callable=check_table_data,
        #     provide_context=True,
        #     op_args=[self.target_db, *self.table_config.tables],
        #     op_kwargs={'allow_null_columns': self.allow_null_columns},
        # )
        #
        # _swap_dataset_tables = PythonOperator(
        #     task_id="swap-dataset-table",
        #     python_callable=swap_dataset_tables,
        #     execution_timeout=timedelta(minutes=10),
        #     provide_context=True,
        #     op_args=[self.target_db, *self.table_config.tables],
        # )
        #
        # _drop_temp_tables = PythonOperator(
        #     task_id="drop-temp-tables",
        #     python_callable=drop_temp_tables,
        #     execution_timeout=timedelta(minutes=10),
        #     provide_context=True,
        #     trigger_rule="one_failed",
        #     op_args=[self.target_db, *self.table_config.tables],
        # )
        #
        # _drop_swap_tables = PythonOperator(
        #     task_id="drop-swap-tables",
        #     python_callable=drop_swap_tables,
        #     execution_timeout=timedelta(minutes=10),
        #     provide_context=True,
        #     op_args=[self.target_db, *self.table_config.tables],
        # )

        # _fetch_data

        (_fetch_data
         >> _build_model
         # >> _measure_model
         # >> _create_tables
         # >> _insert_into_temp_table
         # >> _check_tables
         # >> _swap_dataset_tables
         # >> _drop_swap_tables
         )


        # _insert_into_temp_table >> _drop_temp_tables

        # for dependency in self.dependencies:
        #     sensor = ExternalTaskSensor(
        #         task_id=f'wait-for-{dependency.__name__.lower()}',
        #         pool="sensors",
        #         external_dag_id=dependency.__name__,
        #         external_task_id='swap-dataset-table',
        #         execution_delta=self.dependencies_execution_delta,
        #         timeout=self.dependencies_timeout,
        #         dag=dag,
        #     )
        #
        #     sensor >> [_fetch_model, _fetch_data, _create_tables]

        return dag




class TagsClassifierTrainPipeline(_TrainModelPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table_config = TableConfig(
            table_name=f'{self.controller_pipeline.table_config.table_name}_with_tags',

            field_mapping=[
                ("id", sa.Column("id", sa.Text, primary_key=True)),
                ("policy_feedback_notes", sa.Column("policy_feedback_notes", sa.Text)),
                ("tags_prediction", sa.Column("tags_prediction", sa.Text)),
            ],
        )

        self.start_date = self.controller_pipeline.start_date
        self.schedule_interval = self.controller_pipeline.schedule_interval

    controller_pipeline = InteractionsDatasetPipeline
    # dependencies = [InteractionsDatasetPipeline]

    today = datetime.date.today()
    six_week_ago = today - datetime.timedelta(weeks=6)

    ##todo - add 6 weeks' data only
    query = f"""
             SELECT id, policy_feedback_notes, biu_issue_type  FROM "public".interaction_all_data
                          where policy_feedback_notes!='' 
             """
    # and modified_on >= '{six_week_ago}'



    def fetch_data_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='fetch-interaction-data',
            python_callable=fetch_interaction_labelled_data,
            op_args=[
                self.target_db,
                self.query
            ],
        )

    def build_model_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='build-model',
            python_callable=build_models_pipeline,
            provide_context=True

        )

    # def prediction_operator(self) -> PythonOperator:
    #     return PythonOperator(
    #         task_id='predict-tags',
    #         python_callable=predict_tags,
    #         provide_context=True,
    #         op_args=[
    #             'models'
    #         ],
    #     )
    #
    # def write_prediction_operator(self) -> PythonOperator:
    #     return PythonOperator(
    #         task_id='write-prediction',
    #         python_callable=write_prediction,
    #         provide_context=True,
    #         op_args=[
    #             # self.controller_pipeline.table_config.table_name+'_with_tags'
    #             self.table_config.table_name
    #         ],
    #     )
    #
    #

