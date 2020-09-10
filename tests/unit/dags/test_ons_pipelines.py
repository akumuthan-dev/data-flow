from dataflow.dags.ons_pipelines import (
    ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline,
)
from tests.unit.utils import get_polling_dag_tasks


class TestONSUKTradeInGoodsByCountryAndCommodity:
    def test_tasks_in_dag(self):
        dag = ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline().get_dag()

        assert [t.task_id for t in dag.tasks] == get_polling_dag_tasks()
