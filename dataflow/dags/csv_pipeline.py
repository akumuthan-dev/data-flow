import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagBag
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.operators.csv_outputs import create_csv

DAG_SUBDIR = 'csv_pipelines'


class BaseCSVPipeline:
    target_db = config.DATASETS_DB_NAME
    start_date = datetime(2020, 1, 1)
    end_date = None
    catchup = True
    static = False
    timestamp_output = True
    refresh_daily = True

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
            PythonOperator(
                task_id=f'create-csv-current',
                python_callable=create_csv,
                provide_context=True,
                op_args=[
                    self.target_db,
                    self.base_file_name,
                    self.timestamp_output,
                    self.query,
                ],
                dag=dag,
            )
        return dag


# Import the dags from the csv pipeline sub directory
dag_bag = DagBag(os.path.join(os.path.dirname(os.path.realpath(__name__)), DAG_SUBDIR))
for dag_id, dag in dag_bag.dags.items():
    globals()[dag_id] = dag
