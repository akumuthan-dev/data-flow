# This comment brought to you by the ** airflow DAG ** discovery process

from datetime import datetime


from dataflow.dags import _CSVPipelineDAG
from dataflow.dags.ons_parsing_pipelines import (
    ONSUKTradeInServicesByPartnerCountryNSAPipeline,
    ONSUKTotalTradeAllCountriesNSA,
    ONSUKTradeInGoodsByCountryAndCommodity,
)
from dataflow.dags.ons_pipelines import ONSUKSATradeInGoodsPipeline


class ONSUKSATradeInGoodsCSV(_CSVPipelineDAG):
    schedule_interval = ONSUKSATradeInGoodsPipeline.schedule_interval

    start_date = datetime(2020, 4, 1)
    catchup = False
    static = True

    dependencies = [ONSUKSATradeInGoodsPipeline]

    base_file_name = "ons_uk_sa_trade_in_goods"
    timestamp_output = False

    query = """
SELECT
    ons_geography_code AS ons_iso_alpha_2_code,
    geography AS ons_region_name,
    period,
    period_type,
    CASE
        WHEN direction = 'Imports' THEN 'imports'
        WHEN direction = 'Exports' THEN 'exports'
        ELSE direction
    END AS direction,
    'goods' as trade_type,
    trade_value,
    CASE
        WHEN LOWER(unit) = 'gbp million' THEN 'gbp-million'
        ELSE LOWER(unit)
    END AS unit,
    marker
FROM (
    SELECT
        geography_code as ons_geography_code,
        geography_name as geography,
        period,
        CASE
            WHEN LENGTH(period) = 4 THEN 'year'
            ELSE 'month'
        END AS period_type,
        direction,
        total as trade_value,
        unit,
        '' as marker
    FROM ons_uk_sa_trade_in_goods
    UNION (
        SELECT
            i.geography_code as ons_geography_code,
            i.geography_name as geography,
            i.period,
            CASE
                WHEN LENGTH(i.period) = 4 THEN 'year'
                ELSE 'month'
            END AS period_type,
            'total trade' as direction,
            e.total + i.total as trade_value,
            i.unit,
            'derived' as marker
        FROM ons_uk_sa_trade_in_goods e inner join ons_uk_sa_trade_in_goods i
        ON i.geography_code = e.geography_code AND i.period = e.period
        WHERE i.direction = 'Imports' AND e.direction = 'Exports'
    ) UNION (
        select
            i.geography_code as ons_geography_code,
            i.geography_name as geography,
            i.period,
            CASE
                WHEN LENGTH(i.period) = 4 THEN 'year'
                ELSE 'month'
            END AS period_type,
            'trade balance' as direction,
            e.total - i.total as trade_value,
            i.unit,
            'derived' as marker
        FROM ons_uk_sa_trade_in_goods e inner join ons_uk_sa_trade_in_goods i
        ON i.geography_code = e.geography_code AND i.period = e.period
        WHERE i.direction = 'Imports' AND e.direction = 'Exports'
    ) UNION (
        SELECT
            geography_code as ons_geography_code,
            geography_name as geography,
            period,
            '12 months ending' as period_type,
            direction,
            sum(total) over w AS trade_value,
            unit,
            'derived' as marker
        FROM ons_uk_sa_trade_in_goods
        WHERE char_length(period) = 7
        GROUP BY ons_geography_code, geography, period, direction, total, unit
        WINDOW w AS (
                PARTITION BY geography_name, direction
                ORDER BY geography_name, period ASC
                ROWS between 11 preceding and current row)
    ) UNION (
        SELECT
            i.geography_code as ons_geography_code,
            i.geography_name as geography,
            i.period,
            '12 months ending' as period_type,
            'trade balance' as direction,
            sum(e.total - i.total) over w AS trade_value,
            i.unit,
            'derived' as marker
        FROM ons_uk_sa_trade_in_goods e inner join ons_uk_sa_trade_in_goods i
        ON i.geography_code = e.geography_code AND i.period = e.period
        WHERE i.direction = 'Imports' AND e.direction = 'Exports'
            AND char_length(i.period) = 7
        GROUP BY ons_geography_code, geography, i.period, i.unit, e.total, i.total
        WINDOW w AS (
                PARTITION BY i.geography_name
                ORDER BY i.geography_name, i.period ASC
                ROWS between 11 preceding and current row)
    ) UNION (
        SELECT
            i.geography_code as ons_geography_code,
            i.geography_name as geography,
            i.period,
            '12 months ending' as period_type,
            'total trade' as direction,
            sum(e.total + i.total) over w AS trade_value,
            i.unit,
            'derived' as marker
        FROM ons_uk_sa_trade_in_goods e inner join ons_uk_sa_trade_in_goods i
        ON i.geography_code = e.geography_code AND i.period = e.period
        WHERE i.direction = 'Imports' AND e.direction = 'Exports'
            AND char_length(i.period) = 7
        GROUP BY ons_geography_code, geography, i.period, i.unit, e.total, i.total
        WINDOW w AS (
                PARTITION BY i.geography_name
                ORDER BY i.geography_name, i.period ASC
                ROWS between 11 preceding and current row)
    )
    ORDER BY geography, period, direction
) AS query WHERE (period > '1998-11') OR period_type != '12 months ending'
    """


class ONSUKTradeInServicesByPartnerCountryNSACSV(_CSVPipelineDAG):
    schedule_interval = (
        ONSUKTradeInServicesByPartnerCountryNSAPipeline.schedule_interval
    )

    start_date = datetime(2020, 4, 1)
    catchup = False
    static = True

    dependencies = [ONSUKTradeInServicesByPartnerCountryNSAPipeline]

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
        og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
        og_ons_region_name as ons_region_name,
        norm_period as period,
        norm_period_type as period_type,
        og_direction as direction,
        og_product_code as product_code,
        og_product_name as product_name,
        norm_total as trade_value,
        og_unit as unit,
        og_marker as marker
    FROM ons__uk_trade_in_services_by_country_nsa
    UNION (
        SELECT
            og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
            og_ons_region_name as ons_region_name,
            norm_period as period,
            '4 quarters ending' as period_type,
            og_direction as direction,
            og_product_code as product_code,
            og_product_name as product_name,
            sum(norm_total) over w AS trade_value,
            og_unit as unit,
            'derived' as marker
        FROM ons__uk_trade_in_services_by_country_nsa
        WHERE norm_period_type = 'quarter'
        GROUP BY ons_iso_alpha_2_code, ons_region_name, period, direction, product_code, product_name, norm_total, unit, marker
        WINDOW w AS (
            PARTITION BY og_ons_iso_alpha_2_code, og_direction, og_product_code
            ORDER BY og_ons_iso_alpha_2_code, og_direction, og_product_code, norm_period ASC
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
    geography_code AS ons_iso_alpha_2_code,
    geography_name AS ons_region_name,
    period,
    period_type,
    direction,
    product_name AS trade_type,
    total AS trade_value,
    unit,
    marker
FROM (
    SELECT
        geography_code,
        geography_name,
        period,
        period_type,
        direction,
        product_name,
        total,
        unit,
        marker
    FROM ons_uk_total_trade_all_countries_nsa
    UNION (
        SELECT
            i.geography_code,
            i.geography_name,
            i.period,
            i.period_type,
            'total trade' AS direction,
            i.product_name,
            e.total + i.total AS total,
            i.unit,
            'derived' AS marker
        FROM ons_uk_total_trade_all_countries_nsa e inner join ons_uk_total_trade_all_countries_nsa i
        ON i.geography_code = e.geography_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports'
    ) UNION (
        SELECT
            i.geography_code,
            i.geography_name,
            i.period,
            i.period_type,
            'trade balance' AS direction,
            i.product_name,
            e.total - i.total AS total,
            i.unit,
            'derived' AS marker
        FROM ons_uk_total_trade_all_countries_nsa e inner join ons_uk_total_trade_all_countries_nsa i
        ON i.geography_code = e.geography_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports'
    ) UNION (
        SELECT
            geography_code,
            geography_name,
            period,
            '4 quarters ending' AS period_type,
            direction,
            product_name,
            sum(total) over w AS trade_value,
            unit,
            'derived' AS marker
        FROM ons_uk_total_trade_all_countries_nsa
        WHERE period_type = 'quarter'
        GROUP BY geography_code, geography_name, period, direction, product_name, total, unit, marker
        WINDOW w AS (
            PARTITION BY geography_code, direction, product_name
            ORDER BY geography_code, direction, product_name, period ASC
            ROWS between 3 preceding and current row
        )
        ORDER BY geography_name, period, direction, product_name
    ) UNION (
        SELECT
            i.geography_code,
            i.geography_name,
            i.period,
            '4 quarters ending' AS period_type,
            'trade balance' AS direction,
            i.product_name,
            sum(e.total - i.total) over w AS total,
            i.unit,
            'derived' AS marker
        FROM ons_uk_total_trade_all_countries_nsa e inner join ons_uk_total_trade_all_countries_nsa i
        ON i.geography_code = e.geography_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports' AND i.period_type = 'quarter'
        GROUP BY i.geography_code, i.geography_name, i.period, i.direction, i.product_name, e.total, i.unit, i.total
        WINDOW w AS (
                PARTITION BY i.geography_name, i.direction, i.product_name
                ORDER BY i.geography_name, i.direction, i.product_name, i.period ASC
                ROWS between 3 preceding and current row)
    ) UNION (
        SELECT
            i.geography_code,
            i.geography_name,
            i.period,
            '4 quarters ending' AS period_type,
            'total trade' AS direction,
            i.product_name,
            sum(e.total + i.total) over w AS total,
            i.unit,
            'derived' AS marker
        FROM ons_uk_total_trade_all_countries_nsa e inner join ons_uk_total_trade_all_countries_nsa i
        ON i.geography_code = e.geography_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports' AND i.period_type = 'quarter'
        GROUP BY i.geography_code, i.geography_name, i.period, i.direction, i.product_name, e.total, i.unit, i.total
        WINDOW w AS (
                PARTITION BY i.geography_name, i.direction, i.product_name
                ORDER BY i.geography_name, i.direction, i.product_name, i.period ASC
                ROWS between 3 preceding and current row)
    )
    ORDER BY geography_name, period, period_type, direction, product_name
) AS query WHERE period_type != '4 quarters ending' OR period >= '2016-Q4'
"""


class ONSUKTradeInGoodsByCountryAndCommodityCSVPipeline(_CSVPipelineDAG):
    dependencies = [ONSUKTradeInGoodsByCountryAndCommodity]
    start_date = ONSUKTradeInGoodsByCountryAndCommodity.start_date
    schedule_interval = ONSUKTradeInGoodsByCountryAndCommodity.schedule_interval

    catchup = False
    static = True

    base_file_name = "ons_uk_trade_in_goods_by_country_commodity"
    timestamp_output = False

    query = """
WITH all_rows_plus_balances AS (
    SELECT imports_t.geography_code,
           imports_t.geography_name,
           imports_t.product_code,
           imports_t.product_name,
           imports_t.period,
           imports_t.period_type,
           unnest(array[imports_t.direction, exports_t.direction, 'trade balance', 'total trade']) AS direction,
           unnest(array[imports_t.total, exports_t.total, exports_t.total - imports_t.total, exports_t.total + imports_t.total]) AS total,
           imports_t.unit,
           unnest(array[imports_t.marker, exports_t.marker, 'derived', 'derived']) AS marker
    FROM ons_uk_trade_in_goods_by_country_commodity AS imports_t
    JOIN ons_uk_trade_in_goods_by_country_commodity AS exports_t ON imports_t.geography_code = exports_t.geography_code AND imports_t.product_code = exports_t.product_code AND imports_t.period = exports_t.period AND imports_t.direction = 'imports' AND exports_t.direction = 'exports'
),
     rolling_totals AS (
     SELECT geography_code,
         geography_name,
         product_code,
         product_name,
         period,
         '12 months ending' AS period_type,
         direction,
         sum(total) over (PARTITION
             BY
             geography_code,
             product_code,
             direction
             ORDER BY
                 geography_code,
                 product_code,
                 direction,
                 period ASC rows between 11 preceding and current row) AS total,
         unit,
         'derived' AS marker
     FROM all_rows_plus_balances
     WHERE period_type = 'month'
     GROUP BY geography_code,
              geography_name,
              product_code,
              product_name,
              direction,
              period,
              period_type,
              total,
              unit,
              marker)
SELECT
    geography_code AS ons_iso_alpha_2_code,
    geography_name AS ons_region_name,
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
    ORDER BY geography_name,
             product_code,
             period,
             period_type,
             direction
) AS query
"""
    compress = True
