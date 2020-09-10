from dataflow.dags.ons_parsing_pipelines import (
    ONSUKTradeInServicesByPartnerCountryNSAPipeline,
    ONSUKTotalTradeAllCountriesNSA,
)
from tests.unit.utils import get_base_dag_tasks


class TestONSUKTradeInServicesByPartnerCountryNSAPipeline:
    def test_tasks_in_dag(self):
        dag = ONSUKTradeInServicesByPartnerCountryNSAPipeline().get_dag()

        assert {t.task_id for t in dag.tasks} == get_base_dag_tasks(
            with_modified_date_check=True, fetch_name="run-ons-parser-script"
        )


class TestONSUKTotalTradeAllCountriesNSA:
    def test_tasks_in_dag(self):
        dag = ONSUKTotalTradeAllCountriesNSA().get_dag()

        assert {t.task_id for t in dag.tasks} == get_base_dag_tasks(
            with_modified_date_check=True, fetch_name="run-ons-parser-script"
        )
