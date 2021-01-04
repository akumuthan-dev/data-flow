from dataflow.dags.ons_pipelines import (
    ONSUKSATradeInGoodsPollingPipeline,
    ONSUKTotalTradeAllCountriesNSAPollingPipeline,
    ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline,
    ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline,
)
from tests.unit.utils import get_polling_dag_tasks


class TestONSUKSATradeInGoodsPollingPipeline:
    def test_tasks_in_dag(self):
        dag = ONSUKSATradeInGoodsPollingPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_polling_dag_tasks(
            with_emails=True, with_triggered_dags=["ONSUKSATradeInGoodsCSV"]
        )


class TestONSUKTradeInGoodsByCountryAndCommodity:
    def test_tasks_in_dag(self):
        dag = ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_polling_dag_tasks(
            with_emails=True,
            with_triggered_dags=["ONSUKTradeInGoodsByCountryAndCommodityCSVPipeline"],
        )


class TestONSUKTradeInServicesByPartnerCountryNSAPollingPipeline:
    def test_tasks_in_dag(self):
        dag = ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_polling_dag_tasks(
            with_emails=True,
            with_triggered_dags=["ONSUKTradeInServicesByPartnerCountryNSACSV"],
        )


class TestONSUKTotalTradeAllCountriesNSAPollingPipeline:
    def test_tasks_in_dag(self):
        dag = ONSUKTotalTradeAllCountriesNSAPollingPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_polling_dag_tasks(
            with_emails=True,
            with_triggered_dags=["ONSUKTotalTradeAllCountriesNSACSVPipeline"],
        )
