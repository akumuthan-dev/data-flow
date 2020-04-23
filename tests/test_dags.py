from airflow.models.dagbag import DagBag


def test_pipelines_dags():
    dagbag = DagBag('dataflow')

    assert dagbag.size() == 80
