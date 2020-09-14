# This comment brought to you by the ** airflow DAG ** discovery process

from datetime import datetime


from dataflow.dags import _CSVPipelineDAG
from dataflow.dags.ons_parsing_pipelines import ONSUKTotalTradeAllCountriesNSA
from dataflow.dags.ons_pipelines import (
    ONSUKSATradeInGoodsPollingPipeline,
    ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline,
    ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline,
)


class ONSUKSATradeInGoodsCSV(_CSVPipelineDAG):
    schedule_interval = ONSUKSATradeInGoodsPollingPipeline.schedule_interval

    start_date = datetime(2020, 4, 1)
    catchup = False
    static = True

    dependencies = [ONSUKSATradeInGoodsPollingPipeline]

    base_file_name = "ons_uk_sa_trade_in_goods"
    timestamp_output = False

    query = """
SELECT
    ons_iso_alpha_2_code,
    ons_region_name,
    period,
    period_type,
    direction,
    'goods' as trade_type,
    trade_value,
    unit,
    marker
FROM (
    SELECT
        ons_iso_alpha_2_code,
        ons_region_name,
        period,
        period_type,
        direction,
        value as trade_value,
        unit,
        marker
    FROM ons.uk_sa_trade_in_goods
    UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            i.period_type,
            'total trade' as direction,
            e.value + i.value as trade_value,
            i.unit,
            'derived' as marker
        FROM ons.uk_sa_trade_in_goods e inner join ons.uk_sa_trade_in_goods i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period
        WHERE i.direction = 'imports' AND e.direction = 'exports'
    ) UNION (
        select
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            i.period_type,
            'trade balance' as direction,
            e.value - i.value as trade_value,
            i.unit,
            'derived' as marker
        FROM ons.uk_sa_trade_in_goods e inner join ons.uk_sa_trade_in_goods i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period
        WHERE i.direction = 'imports' AND e.direction = 'exports'
    ) UNION (
        SELECT
            ons_iso_alpha_2_code,
            ons_region_name,
            period,
            '12 months ending' as period_type,
            direction,
            sum(value) over w AS trade_value,
            unit,
            'derived' as marker
        FROM ons.uk_sa_trade_in_goods
        WHERE period_type = 'month'
        GROUP BY ons_iso_alpha_2_code, ons_region_name, period, direction, value, unit
        WINDOW w AS (
                PARTITION BY ons_region_name, direction
                ORDER BY ons_region_name, period ASC
                ROWS between 11 preceding and current row)
    ) UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            '12 months ending' as period_type,
            'trade balance' as direction,
            sum(e.value - i.value) over w AS trade_value,
            i.unit,
            'derived' as marker
        FROM ons.uk_sa_trade_in_goods e inner join ons.uk_sa_trade_in_goods i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period
        WHERE i.direction = 'imports' AND e.direction = 'exports'
            AND i.period_type = 'month'
        GROUP BY i.ons_iso_alpha_2_code, i.ons_region_name, i.period, i.unit, e.value, i.value
        WINDOW w AS (
                PARTITION BY i.ons_region_name
                ORDER BY i.ons_region_name, i.period ASC
                ROWS between 11 preceding and current row)
    ) UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            '12 months ending' as period_type,
            'total trade' as direction,
            sum(e.value + i.value) over w AS trade_value,
            i.unit,
            'derived' as marker
        FROM ons.uk_sa_trade_in_goods e inner join ons.uk_sa_trade_in_goods i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period
        WHERE i.direction = 'imports' AND e.direction = 'exports'
            AND i.period_type = 'month'
        GROUP BY i.ons_iso_alpha_2_code, i.ons_region_name, i.period, i.unit, e.value, i.value
        WINDOW w AS (
                PARTITION BY i.ons_region_name
                ORDER BY i.ons_region_name, i.period ASC
                ROWS between 11 preceding and current row)
    )
    ORDER BY ons_region_name, period, direction
) AS query WHERE (period > '1998-11') OR period_type != '12 months ending'
    """


class ONSUKTradeInServicesByPartnerCountryNSACSV(_CSVPipelineDAG):
    schedule_interval = (
        ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline.schedule_interval
    )

    start_date = datetime(2020, 4, 1)
    catchup = False
    static = True

    dependencies = [ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline]

    base_file_name = "ons_uk_trade_in_services_by_country_nsa"
    timestamp_output = False

    query = """
SELECT
    ons_iso_alpha_2_code,
    ons_region_name,
    period,
    period_type,
    direction,
    product_code,
    product_name,
    'services' as trade_type,
    trade_value,
    unit,
    marker
FROM (
    SELECT
        ons_iso_alpha_2_code,
        ons_region_name,
        period,
        period_type,
        direction,
        product_code,
        product_name,
        total as trade_value,
        unit,
        marker
    FROM ons.uk_trade_in_services_by_country_nsa
    UNION (
        SELECT
            ons_iso_alpha_2_code,
            ons_region_name,
            period,
            '4 quarters ending' as period_type,
            direction,
            product_code,
            product_name,
            sum(total) over w AS trade_value,
            unit,
            'derived' as marker
        FROM ons.uk_trade_in_services_by_country_nsa
        WHERE period_type = 'quarter'
        GROUP BY ons_iso_alpha_2_code, ons_region_name, period, direction, product_code, product_name, total, unit, marker
        WINDOW w AS (
            PARTITION BY ons_iso_alpha_2_code, direction, product_code
            ORDER BY ons_iso_alpha_2_code, direction, product_code, period ASC
            ROWS between 3 preceding and current row
        )
        ORDER BY ons_region_name, period, direction, product_code
    )
    ORDER BY ons_region_name, period, direction, product_code
) AS query WHERE period_type != '4 quarters ending' OR period >= '2016-Q4'
"""


class ONSUKTotalTradeAllCountriesNSACSVPipeline(_CSVPipelineDAG):
    dependencies = [ONSUKTotalTradeAllCountriesNSA]
    start_date = ONSUKTotalTradeAllCountriesNSA.start_date
    schedule_interval = ONSUKTotalTradeAllCountriesNSA.schedule_interval

    catchup = False
    static = True

    base_file_name = "ons_uk_total_trade_all_countries_nsa"
    timestamp_output = False

    query = """
SELECT
    ons_iso_alpha_2_code AS ons_iso_alpha_2_code,
    ons_region_name AS ons_region_name,
    period,
    period_type,
    direction,
    product_name AS trade_type,
    total AS trade_value,
    unit,
    marker
FROM (
    SELECT
        og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
        og_ons_region_name as ons_region_name,
        norm_period as period,
        norm_period_type as period_type,
        og_direction as direction,
        og_product_name as product_name,
        norm_total as total,
        og_unit as unit,
        norm_marker as marker
    FROM ons__uk_total_trade_all_countries_nsa
    UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
            i.og_ons_region_name as ons_region_name,
            i.norm_period as period,
            i.norm_period_type as period_type,
            'total trade' AS direction,
            i.og_product_name as product_name,
            e.norm_total + i.norm_total AS total,
            i.og_unit as unit,
            'derived' AS marker
        FROM ons__uk_total_trade_all_countries_nsa e inner join ons__uk_total_trade_all_countries_nsa i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period AND i.og_product_name = e.og_product_name
        WHERE e.og_direction = 'exports' AND i.og_direction = 'imports'
    ) UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
            i.og_ons_region_name as ons_region_name,
            i.norm_period as period,
            i.norm_period_type as period_type,
            'trade balance' AS direction,
            i.og_product_name as product_name,
            e.norm_total - i.norm_total AS total,
            i.og_unit as unit,
            'derived' AS marker
        FROM ons__uk_total_trade_all_countries_nsa e inner join ons__uk_total_trade_all_countries_nsa i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period AND i.og_product_name = e.og_product_name
        WHERE e.og_direction = 'exports' AND i.og_direction = 'imports'
    ) UNION (
        SELECT
            og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
            og_ons_region_name as ons_region_name,
            norm_period as period,
            '4 quarters ending' AS period_type,
            og_direction as direction,
            og_product_name as product_name,
            sum(norm_total) over w AS trade_value,
            og_unit as unit,
            'derived' AS marker
        FROM ons__uk_total_trade_all_countries_nsa
        WHERE norm_period_type = 'quarter'
        GROUP BY og_ons_iso_alpha_2_code, og_ons_region_name, norm_period, og_direction, og_product_name, norm_total, og_unit, marker
        WINDOW w AS (
            PARTITION BY og_ons_iso_alpha_2_code, og_direction, og_product_name
            ORDER BY og_ons_iso_alpha_2_code, og_direction, og_product_name, norm_period ASC
            ROWS between 3 preceding and current row
        )
    ) UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
            i.og_ons_region_name as ons_region_name,
            i.norm_period as period,
            '4 quarters ending' AS period_type,
            'trade balance' AS direction,
            i.og_product_name as product_name,
            sum(e.norm_total - i.norm_total) over w AS total,
            i.og_unit as unit,
            'derived' AS marker
        FROM ons__uk_total_trade_all_countries_nsa e inner join ons__uk_total_trade_all_countries_nsa i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period AND i.og_product_name = e.og_product_name
        WHERE e.og_direction = 'exports' AND i.og_direction = 'imports' AND i.norm_period_type = 'quarter'
        GROUP BY i.og_ons_iso_alpha_2_code, i.og_ons_region_name, i.norm_period, i.og_direction, i.og_product_name, e.norm_total, i.og_unit, i.norm_total
        WINDOW w AS (
                PARTITION BY i.og_ons_region_name, i.og_direction, i.og_product_name
                ORDER BY i.og_ons_region_name, i.og_direction, i.og_product_name, i.norm_period ASC
                ROWS between 3 preceding and current row)
    ) UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
            i.og_ons_region_name as ons_region_name,
            i.norm_period as period,
            '4 quarters ending' AS period_type,
            'total trade' AS direction,
            i.og_product_name as product_name,
            sum(e.norm_total + i.norm_total) over w AS total,
            i.og_unit as unit,
            'derived' AS marker
        FROM ons__uk_total_trade_all_countries_nsa e inner join ons__uk_total_trade_all_countries_nsa i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period AND i.og_product_name = e.og_product_name
        WHERE e.og_direction = 'exports' AND i.og_direction = 'imports' AND i.norm_period_type = 'quarter'
        GROUP BY i.og_ons_iso_alpha_2_code, i.og_ons_region_name, i.norm_period, i.og_direction, i.og_product_name, e.norm_total, i.og_unit, i.norm_total
        WINDOW w AS (
                PARTITION BY i.og_ons_region_name, i.og_direction, i.og_product_name
                ORDER BY i.og_ons_region_name, i.og_direction, i.og_product_name, i.norm_period ASC
                ROWS between 3 preceding and current row)
    )
    ORDER BY ons_region_name, period, period_type, direction, product_name
) AS query WHERE period_type != '4 quarters ending' OR period >= '2016-Q4'
"""


class ONSUKTradeInGoodsByCountryAndCommodityCSVPipeline(_CSVPipelineDAG):
    dependencies = [ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline]
    start_date = ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline.start_date
    schedule_interval = (
        ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline.schedule_interval
    )

    catchup = False
    static = True

    base_file_name = "ons_uk_trade_in_goods_by_country_commodity"
    timestamp_output = False

    query = """
WITH all_rows_plus_balances AS (
    SELECT imports_t.ons_iso_alpha_2_code as ons_iso_alpha_2_code,
           imports_t.ons_region_name as ons_region_name,
           imports_t.product_code as product_code,
           imports_t.product_name as product_name,
           imports_t.period as period,
           imports_t.period_type as period_type,
           unnest(array[imports_t.direction, exports_t.direction, 'trade balance', 'total trade']) AS direction,
           unnest(array[imports_t.total, exports_t.total, exports_t.total - imports_t.total, exports_t.total + imports_t.total]) AS total,
           imports_t.unit as unit,
           unnest(array[imports_t.marker, exports_t.marker, 'derived', 'derived']) AS marker
    FROM ons.uk_trade_in_goods_by_country_and_commodity AS imports_t
    JOIN ons.uk_trade_in_goods_by_country_and_commodity AS exports_t ON imports_t.ons_iso_alpha_2_code = exports_t.ons_iso_alpha_2_code AND imports_t.product_code = exports_t.product_code AND imports_t.period = exports_t.period AND imports_t.direction = 'imports' AND exports_t.direction = 'exports'
),
     rolling_totals AS (
     SELECT ons_iso_alpha_2_code,
         ons_region_name,
         product_code,
         product_name,
         period,
         '12 months ending' AS period_type,
         direction,
         sum(total) over (PARTITION
             BY
             ons_iso_alpha_2_code,
             product_code,
             direction
             ORDER BY
                 ons_iso_alpha_2_code,
                 product_code,
                 direction,
                 period ASC rows between 11 preceding and current row) AS total,
         unit,
         'derived' AS marker
     FROM all_rows_plus_balances
     WHERE period_type = 'month'
     GROUP BY ons_iso_alpha_2_code,
              ons_region_name,
              product_code,
              product_name,
              direction,
              period,
              period_type,
              total,
              unit,
              marker)
SELECT
    ons_iso_alpha_2_code,
    ons_region_name,
    period,
    period_type,
    direction,
    product_code,
    product_name,
    'goods' AS trade_type,
    total AS trade_value,
    unit,
    marker
FROM (
    SELECT *
    FROM all_rows_plus_balances
    UNION ALL
    (
        SELECT *
        FROM rolling_totals
        WHERE period >= '1998-12'
    )
    ORDER BY ons_region_name,
             product_code,
             period,
             period_type,
             direction
) AS query
"""
    compress = True
