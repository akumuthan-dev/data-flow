from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow.dags.base import PipelineMeta
from dataflow.operators.tensorflow import example_tensorflow


class ExampleTensorflowPipeline(metaclass=PipelineMeta):
    """Example Tensorflow pipeline

    It's not expected that Tensorflow pipelines will inherit from this class, it's just to make
    sure we can run pipelines that use Tensorflow in production
    """

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=False,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2019, 1, 1),
            },
            start_date=datetime(2019, 11, 5),
            end_date=None,
            schedule_interval="@daily",
        )
        PythonOperator(
            task_id="example-tensorflow",
            python_callable=example_tensorflow,
            provide_context=True,
            queue='tensorflow',
            dag=dag,
        )
        return dag
