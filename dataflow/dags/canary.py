from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import config

dag = DAG(
    dag_id='Canary',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    start_date=datetime(2019, 11, 5),
    end_date=None,
    schedule_interval='@hourly',
    max_active_runs=1,
)


def canary_tweet(**kwargs):
    """Log that this task was processed"""
    return f'Canary task {kwargs["task_name"]} was processed successfully'


for index in range(config.INGEST_TASK_CONCURRENCY):
    run_this = PythonOperator(
        task_id=f'canary-tweet-{index}',
        python_callable=canary_tweet,
        provide_context=True,
        dag=dag,
        op_kwargs={'task_name': f'canary-tweet-{index}'},
    )
