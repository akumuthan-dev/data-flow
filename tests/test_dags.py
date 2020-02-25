import pytest
from airflow.models import DAG

from dataflow.dags import (
    activity_stream_pipelines,
    canary,
    company_matching,
    dataset_pipelines,
    ons_pipelines,
    spi_pipeline,
)
from dataflow.dags.csv_pipelines import (
    csv_pipelines_daily,
    csv_pipelines_monthly,
    csv_pipelines_yearly,
    csv_refresh_pipeline,
)


def test_canary_dag():
    assert isinstance(canary.dag, DAG)


def test_canary_tweets():
    assert (
        canary.canary_tweet(task_name='test')
        == 'Canary task test was processed successfully'
    )


@pytest.mark.parametrize(
    "module",
    [
        activity_stream_pipelines,
        company_matching,
        csv_pipelines_daily,
        csv_pipelines_monthly,
        csv_pipelines_yearly,
        csv_refresh_pipeline,
        dataset_pipelines,
        ons_pipelines,
        spi_pipeline,
    ],
)
def test_pipelines_dags(module):
    dag_vars = [value for name, value in vars(module).items() if name.endswith("__dag")]

    assert dag_vars
    assert all(isinstance(dag, DAG) for dag in dag_vars)
