from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dataflow.dags.csv_pipeline import BaseCSVPipeline
from dataflow.dags.csv_pipelines.csv_pipelines_daily import (
    BaseDailyCSVPipeline,
)  # noqa: F401
from dataflow.dags.csv_pipelines.csv_pipelines_monthly import (
    BaseMonthlyCSVPipeline,
)  # noqa: F401
from dataflow.dags.csv_pipelines.csv_pipelines_yearly import (
    BaseYearlyCSVPipeline,
)  # noqa: F401
from dataflow.operators.csv_outputs import create_csv


class DailyCSVRefreshPipeline(BaseCSVPipeline):
    """
    Dag to rerun all previous runs of non-static csv outputs.
    Runs at 5am daily to recreate all csvs after the nightly table syncs have run
    """

    schedule_interval = '0 5 * * *'
    start_date = datetime(2020, 2, 13)

    @staticmethod
    def _get_pipeline_previous_run_dates(pipeline):
        """
        Get all previous run dataset for a DAG excluding today's run
        """
        return (
            pipeline()
            .get_dag()
            .get_run_dates(
                pipeline.start_date.replace(tzinfo=pytz.UTC),
                datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
                ),
            )
        )

    # Find a list of all dags that need to be run
    def get_dag(self):
        with DAG(
            self.__class__.__name__,
            catchup=True,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'catchup': self.catchup,
                'start_date': datetime(2019, 1, 1),
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
        ) as dag:
            for base_pipeline in BaseCSVPipeline.__subclasses__():
                pipelines = [
                    pipeline
                    for pipeline in base_pipeline.__subclasses__()
                    if not pipeline.static
                ]
                for pipeline in pipelines:
                    run_dates = self._get_pipeline_previous_run_dates(pipeline)
                    task_group = DummyOperator(task_id=pipeline.__name__)
                    tasks = []
                    for run_date in run_dates:
                        tasks.append(
                            PythonOperator(
                                task_id=f'{pipeline.__name__}-{run_date.strftime("%Y-%m-%d")}',
                                python_callable=create_csv,
                                provide_context=True,
                                op_args=[
                                    pipeline.target_db,
                                    pipeline.base_file_name,
                                    pipeline.timestamp_output,
                                    pipeline.query,
                                ],
                                op_kwargs={'run_date': run_date},
                                dag=dag,
                            )
                        )
                    task_group.set_downstream(tasks)
        return dag


globals()['DailyCSVRefreshPipeline__dag'] = DailyCSVRefreshPipeline().get_dag()
