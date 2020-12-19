from typing import Optional

from airflow.operators.python_operator import PythonOperator
import sqlalchemy as sa

from dataflow.dags import _PipelineDAG
from dataflow.dags.base import _FastPollingPipeline
from dataflow.operators.ons import fetch_from_ons_sparql
from dataflow.utils import SingleTableConfig


class _ONSPipeline(_PipelineDAG):
    query: str
    index_query: Optional[str] = None

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id="fetch-from-ons-sparql",
            python_callable=fetch_from_ons_sparql,
            provide_context=True,
            op_args=[self.table_config.table_name, self.query, self.index_query],
            retries=self.fetch_retries,
        )


class ONSUKSATradeInGoodsPollingPipeline(_FastPollingPipeline):
    from dataflow.ons_scripts.uk_trade_in_goods_all_countries_seasonally_adjusted.main import (
        get_current_and_next_release_date,
        get_data,
    )

    date_checker = get_current_and_next_release_date
    data_getter = get_data
    table_config = SingleTableConfig(
        schema='ons',
        table_name="uk_sa_trade_in_goods",
        field_mapping=[
            ('ons_iso_alpha_2_code', sa.Column('ons_iso_alpha_2_code', sa.Text)),
            ('ons_region_name', sa.Column('ons_region_name', sa.Text)),
            ('period', sa.Column('period', sa.Text)),
            ('period_type', sa.Column('period_type', sa.Text)),
            ('direction', sa.Column('direction', sa.Text)),
            ('measure_type', sa.Column('measure_type', sa.Text)),
            ('value', sa.Column('value', sa.Numeric)),
            ('unit', sa.Column('unit', sa.Text)),
            ('marker', sa.Column('marker', sa.Text)),
        ],
    )

    update_emails_data_environment_variable = (
        'UPDATE_EMAILS_DATA__ONSUKSATradeInGoodsPollingPipeline'
    )


class ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline(_FastPollingPipeline):
    from dataflow.ons_scripts.uk_trade_country_by_commodity.main import (
        get_current_and_next_release_date,
        get_data,
    )

    date_checker = get_current_and_next_release_date
    data_getter = get_data
    table_config = SingleTableConfig(
        schema='ons',
        table_name='uk_trade_in_goods_by_country_and_commodity',
        field_mapping=[
            ('ons_iso_alpha_2_code', sa.Column('ons_iso_alpha_2_code', sa.Text)),
            ('ons_region_name', sa.Column('ons_region_name', sa.Text)),
            ('period', sa.Column('period', sa.Text)),
            ('period_type', sa.Column('period_type', sa.Text)),
            ('direction', sa.Column('direction', sa.Text)),
            ('product_code', sa.Column('product_code', sa.Text)),
            ('product_name', sa.Column('product_name', sa.Text)),
            ('seasonal_adjustment', sa.Column('seasonal_adjustment', sa.Text)),
            ('measure_type', sa.Column('measure_type', sa.Text)),
            ('value', sa.Column('value', sa.Numeric)),
            ('unit', sa.Column('unit', sa.Text)),
            ('marker', sa.Column('marker', sa.Text)),
        ],
    )
    worker_queue = 'high-memory-usage'

    update_emails_data_environment_variable = (
        'UPDATE_EMAILS_DATA__ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline'
    )


class ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline(_FastPollingPipeline):
    from dataflow.ons_scripts.uk_trade_in_services_service_type_by_partner_country_non_seasonally_adjusted.main import (
        get_current_and_next_release_date,
        get_data,
    )

    schedule_interval = "0 6 20-31 1,4,7,10 *"

    date_checker = get_current_and_next_release_date
    data_getter = get_data

    table_config = SingleTableConfig(
        schema='ons',
        table_name='uk_trade_in_services_by_country_nsa',
        field_mapping=[
            ('ons_iso_alpha_2_code', sa.Column('ons_iso_alpha_2_code', sa.Text)),
            ('ons_region_name', sa.Column('ons_region_name', sa.Text)),
            ('period', sa.Column('period', sa.Text)),
            ('period_type', sa.Column('period_type', sa.Text)),
            ('direction', sa.Column('direction', sa.Text)),
            ('product_code', sa.Column('product_code', sa.Text)),
            ('product_name', sa.Column('product_name', sa.Text)),
            ('measure_type', sa.Column('measure_type', sa.Text)),
            ('value', sa.Column('value', sa.Numeric)),
            ('unit', sa.Column('unit', sa.Text)),
            ('marker', sa.Column('marker', sa.Text)),
        ],
    )

    update_emails_data_environment_variable = (
        'UPDATE_EMAILS_DATA__ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline'
    )


class ONSUKTotalTradeAllCountriesNSAPollingPipeline(_FastPollingPipeline):
    from dataflow.ons_scripts.uk_total_trade_all_countries_non_seasonally_adjusted.main import (
        get_current_and_next_release_date,
        get_data,
    )

    schedule_interval = "0 6 20-31 1,4,7,10 *"

    date_checker = get_current_and_next_release_date
    data_getter = get_data

    table_config = SingleTableConfig(
        schema='ons',
        table_name="uk_total_trade_all_countries_nsa",
        field_mapping=[
            ('ons_iso_alpha_2_code', sa.Column('ons_iso_alpha_2_code', sa.Text)),
            ('ons_region_name', sa.Column('ons_region_name', sa.Text)),
            ('period', sa.Column('period', sa.Text)),
            ('period_type', sa.Column('period_type', sa.Text)),
            ('direction', sa.Column('direction', sa.Text)),
            ('product_name', sa.Column('product_name', sa.Text)),
            ('measure_type', sa.Column('measure_type', sa.Text)),
            ('value', sa.Column('value', sa.Numeric)),
            ('unit', sa.Column('unit', sa.Text)),
            ('marker', sa.Column('marker', sa.Text)),
        ],
    )

    update_emails_data_environment_variable = (
        'UPDATE_EMAILS_DATA__ONSUKTotalTradeAllCountriesNSAPollingPipeline'
    )
