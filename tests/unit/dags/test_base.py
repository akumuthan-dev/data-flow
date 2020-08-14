from datetime import datetime

from tests.unit.utils import get_base_dag_tasks


def test_base_dag_tasks(test_dag_cls):
    dag = test_dag_cls().get_dag()

    assert [t.task_id for t in dag.tasks] == get_base_dag_tasks(
        with_modified_date_check=False
    )


def test_dag_tasks_with_source_modified_data_utc_callable(test_dag_cls):
    class ModifiedTestDAG(test_dag_cls):
        @staticmethod
        def get_source_data_modified_utc(self) -> datetime:
            return datetime(2020, 1, 1)

        def get_source_data_modified_utc_callable(self):
            return self.get_source_data_modified_utc

    dag = ModifiedTestDAG().get_dag()

    assert [t.task_id for t in dag.tasks] == get_base_dag_tasks(
        with_modified_date_check=True
    )
