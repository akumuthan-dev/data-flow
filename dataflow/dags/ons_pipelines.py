from typing import Optional

from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.dags.base import _FastPollingPipeline
from dataflow.operators.ons import fetch_from_ons_sparql
from dataflow.utils import TableConfig


class _ONSPipeline(_PipelineDAG):
    query: str
    index_query: Optional[str] = None

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-from-ons-sparql",
            python_callable=fetch_from_ons_sparql,
            provide_context=True,
            op_args=[self.table_config.table_name, self.query, self.index_query],
        )


class ONSUKSATradeInGoodsPollingPipeline(_FastPollingPipeline):
    from dataflow.ons_scripts.uk_trade_in_goods_all_countries_seasonally_adjusted.main import (
        get_source_data_modified_date,
        get_data,
    )

    date_checker = get_source_data_modified_date
    data_getter = get_data
    table_config = TableConfig(
        schema='ons', table_name="uk_sa_trade_in_goods", field_mapping=[],
    )

    update_emails_data_environment_variable = (
        'UPDATE_EMAILS_DATA__ONSUKSATradeInGoodsPollingPipeline'
    )


class ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline(_FastPollingPipeline):
    from dataflow.ons_scripts.uktradecountrybycommodity.main import (
        get_source_data_modified_date,
        get_data,
    )

    date_checker = get_source_data_modified_date
    data_getter = get_data
    table_config = TableConfig(
        schema='ons',
        table_name='uk_trade_in_goods_by_country_and_commodity',
        field_mapping=[],
    )
    queue = 'high-memory-usage'
