"""A module that defines Airflow DAGS for Comtrade pipelines"""

from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.operators.comtrade import fetch_comtrade_goods_data
from dataflow.utils import TableConfig


class ComtradeGoodsPipeline(_PipelineDAG):
    table_config = TableConfig(
        schema="un", table_name="comtrade__goods", field_mapping=[],
    )

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-comtrade-services-data",
            python_callable=fetch_comtrade_goods_data,
            provide_context=True,
            op_args=[self.table_config.table_name],  # pylint: disable=no-member
        )
