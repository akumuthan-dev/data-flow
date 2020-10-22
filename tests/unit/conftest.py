from unittest import mock

import pytest
import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.utils import TableConfig


@pytest.fixture
def mock_db_conn(mocker):
    engine = mock.MagicMock()
    mocker.patch("sqlalchemy.create_engine", return_value=engine, autospec=True)

    engine.dialect.identifier_preparer.quote = lambda x: f"QUOTED<{x}>"

    conn = mock.Mock()
    engine.begin.return_value.__enter__.return_value = conn

    return conn


@pytest.fixture()
def test_dag_cls():
    def _fetch(*args, **kwargs):
        return

    class TestDAG(_PipelineDAG):
        table_config = TableConfig(
            table_name="test-table",
            field_mapping=(
                ("id", sa.Column("id", sa.Integer, primary_key=True)),
                ("data", sa.Column("data", sa.Integer)),
            ),
        )

        def get_fetch_operator(self) -> PythonOperator:
            return PythonOperator(
                task_id="fetch-data",
                python_callable=_fetch,
                provide_context=True,
            )

    return TestDAG
