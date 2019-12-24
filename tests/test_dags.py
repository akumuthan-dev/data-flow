import pytest
from airflow.models import DAG

from dataflow.dags import (
    activity_stream_pipelines,
    canary,
    dataset_pipelines,
    ons_pipelines,
    view_pipelines,
)


def test_canary_dag():
    assert isinstance(canary.dag, DAG)


@pytest.mark.parametrize(
    "module",
    [activity_stream_pipelines, dataset_pipelines, ons_pipelines, view_pipelines],
)
def test_pipelines_dags(module):
    dag_vars = [value for name, value in vars(module).items() if name.endswith("__dag")]

    assert dag_vars
    assert all(isinstance(dag, DAG) for dag in dag_vars)
