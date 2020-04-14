from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _CSVPipelineDAG
from dataflow.dags.csv_pipelines.csv_pipelines_daily import (  # noqa: F401
    _DailyCSVPipeline,
)
from dataflow.dags.csv_pipelines.csv_pipelines_monthly import (  # noqa: F401
    _MonthlyCSVPipeline,
)
from dataflow.dags.csv_pipelines.csv_pipelines_yearly import (  # noqa: F401
    _YearlyCSVPipeline,
)
from dataflow.operators.csv_outputs import create_csv


class DailyCSVRefreshPipeline(_CSVPipelineDAG):
    """
    Dag to rerun all previous runs of non-static csv outputs.
    Runs daily to recreate all csvs after the nightly table syncs have run
    """

    schedule_interval = '0 6 * * *'
    start_date = datetime(2020, 2, 13)
    catchup = False

    @staticmethod
    def _get_pipeline_previous_runs(pipeline):
        """
        Return start and end dates for each previous run of a pipeline
        """
        dag = pipeline().get_dag()
        run_dates = dag.get_run_dates(
            pipeline.start_date.replace(tzinfo=pytz.UTC),
            datetime.utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
            ),
        )
        return ((run_date, dag.following_schedule(run_date)) for run_date in run_dates)

    # Find a list of all dags that need to be run
    def get_dag(self):
        with DAG(
            self.__class__.__name__,
            catchup=self.catchup,
            concurrency=1,
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
            for base_pipeline in _CSVPipelineDAG.__subclasses__():
                pipelines = [
                    pipeline
                    for pipeline in base_pipeline.__subclasses__()
                    if not pipeline.static
                ]
                for pipeline in pipelines:
                    previous_runs = self._get_pipeline_previous_runs(pipeline)
                    task_group = DummyOperator(task_id=pipeline.__name__)
                    tasks = []
                    for previous_run in previous_runs:
                        run_date, end_date = previous_run
                        tasks.append(
                            PythonOperator(
                                task_id=f'{pipeline.__name__}-{run_date.strftime("%Y-%m-%d")}',
                                python_callable=create_csv,
                                provide_context=True,
                                params={'run_date': run_date.strftime('%Y-%m-%d')},
                                op_args=[
                                    pipeline.target_db,
                                    pipeline.base_file_name,
                                    pipeline.timestamp_output,
                                    pipeline.query,
                                ],
                                op_kwargs={
                                    'run_date': run_date,
                                    'next_execution_date': end_date,
                                },
                                dag=dag,
                            )
                        )
                    task_group.set_downstream(tasks)
        return dag
