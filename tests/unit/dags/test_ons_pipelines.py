from dataflow.dags.ons_pipelines import (
    ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline,
    ONSUKSATradeInGoodsPollingPipeline,
)
from tests.unit.utils import get_polling_dag_tasks


class TestONSUKSATradeInGoodsPollingPipeline:
    def test_tasks_in_dag(self):
        dag = ONSUKSATradeInGoodsPollingPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_polling_dag_tasks(with_emails=True)


class TestONSUKTradeInGoodsByCountryAndCommodity:
    def test_tasks_in_dag(self):
        dag = ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_polling_dag_tasks(
            with_emails=False
        )
