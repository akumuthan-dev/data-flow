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
    og_period AS period,
    norm_period_type AS period_type,
    CASE
        WHEN og_direction = 'Imports' THEN 'imports'
        WHEN og_direction = 'Exports' THEN 'exports'
        ELSE og_direction
    END AS direction,
    'goods' as trade_type,
    trade_value,
    CASE
        WHEN LOWER(og_unit) = 'gbp million' THEN 'gbp-million'
        ELSE LOWER(og_unit)
    END AS unit,
    marker
FROM (
    SELECT
        og_ons_iso_alpha_2_code as ons_geography_code,
        og_ons_region_name as geography,
        og_period,
        norm_period_type,
        og_direction,
        norm_total as trade_value,
        og_unit,
        norm_marker AS marker
    FROM ons__uk_sa_trade_in_goods
    UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_geography_code,
            i.og_ons_region_name as geography,
            i.og_period,
            i.norm_period_type,
            'total trade' as og_direction,
            e.norm_total + i.norm_total as trade_value,
            i.og_unit,
            'derived' as marker
        FROM ons__uk_sa_trade_in_goods e inner join ons__uk_sa_trade_in_goods i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period
        WHERE i.og_direction = 'Imports' AND e.og_direction = 'Exports'
    ) UNION (
        select
            i.og_ons_iso_alpha_2_code as ons_geography_code,
            i.og_ons_region_name as geography,
            i.og_period,
            i.norm_period_type,
            'trade balance' as og_direction,
            e.norm_total - i.norm_total as trade_value,
            i.og_unit,
            'derived' as marker
        FROM ons__uk_sa_trade_in_goods e inner join ons__uk_sa_trade_in_goods i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period
        WHERE i.og_direction = 'Imports' AND e.og_direction = 'Exports'
    ) UNION (
        SELECT
            og_ons_iso_alpha_2_code as ons_geography_code,
            og_ons_region_name as geography,
            og_period,
            '12 months ending' as norm_period_type,
            og_direction,
            sum(norm_total) over w AS trade_value,
            og_unit,
            'derived' as marker
        FROM ons__uk_sa_trade_in_goods
        WHERE char_length(og_period) = 7
        GROUP BY ons_geography_code, geography, og_period, og_direction, norm_total, og_unit
        WINDOW w AS (
                PARTITION BY og_ons_region_name, og_direction
                ORDER BY og_ons_region_name, og_period ASC
                ROWS between 11 preceding and current row)
    ) UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_geography_code,
            i.og_ons_region_name as geography,
            i.og_period,
            '12 months ending' as norm_period_type,
            'trade balance' as og_direction,
            sum(e.norm_total - i.norm_total) over w AS trade_value,
            i.og_unit,
            'derived' as marker
        FROM ons__uk_sa_trade_in_goods e inner join ons__uk_sa_trade_in_goods i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period
        WHERE i.og_direction = 'Imports' AND e.og_direction = 'Exports'
            AND char_length(i.og_period) = 7
        GROUP BY ons_geography_code, geography, i.og_period, i.og_unit, e.norm_total, i.norm_total
        WINDOW w AS (
                PARTITION BY i.og_ons_region_name
                ORDER BY i.og_ons_region_name, i.og_period ASC
                ROWS between 11 preceding and current row)
    ) UNION (
        SELECT
            i.og_ons_iso_alpha_2_code as ons_geography_code,
            i.og_ons_region_name as geography,
            i.og_period,
            '12 months ending' as norm_period_type,
            'total trade' as og_direction,
            sum(e.norm_total + i.norm_total) over w AS trade_value,
            i.og_unit,
            'derived' as marker
        FROM ons__uk_sa_trade_in_goods e inner join ons__uk_sa_trade_in_goods i
        ON i.og_ons_iso_alpha_2_code = e.og_ons_iso_alpha_2_code AND i.og_period = e.og_period
        WHERE i.og_direction = 'Imports' AND e.og_direction = 'Exports'
            AND char_length(i.og_period) = 7
        GROUP BY ons_geography_code, geography, i.og_period, i.og_unit, e.norm_total, i.norm_total
        WINDOW w AS (
                PARTITION BY i.og_ons_region_name
                ORDER BY i.og_ons_region_name, i.og_period ASC
                ROWS between 11 preceding and current row)
    )
    ORDER BY geography, og_period, og_direction
) AS query WHERE (og_period > '1998-11') OR norm_period_type != '12 months ending'
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
        norm_marker as marker
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
    dependencies = [ONSUKTradeInGoodsByCountryAndCommodity]
    start_date = ONSUKTradeInGoodsByCountryAndCommodity.start_date
    schedule_interval = ONSUKTradeInGoodsByCountryAndCommodity.schedule_interval

    catchup = False
    static = True

    base_file_name = "ons_uk_trade_in_goods_by_country_commodity"
    timestamp_output = False

    query = """
WITH all_rows_plus_balances AS (
    SELECT imports_t.og_ons_iso_alpha_2_code as ons_iso_alpha_2_code,
           imports_t.og_ons_region_name as ons_region_name,
           imports_t.og_product_code as product_code,
           imports_t.og_product_name as product_name,
           imports_t.norm_period as period,
           imports_t.norm_period_type as period_type,
           unnest(array[imports_t.og_direction, exports_t.og_direction, 'trade balance', 'total trade']) AS direction,
           unnest(array[imports_t.norm_total, exports_t.norm_total, exports_t.norm_total - imports_t.norm_total, exports_t.norm_total + imports_t.norm_total]) AS total,
           imports_t.og_unit as unit,
           unnest(array[imports_t.norm_marker, exports_t.norm_marker, 'derived', 'derived']) AS marker
    FROM ons__uk_trade_goods_by_country_commodity AS imports_t
    JOIN ons__uk_trade_goods_by_country_commodity AS exports_t ON imports_t.og_ons_iso_alpha_2_code = exports_t.og_ons_iso_alpha_2_code AND imports_t.og_product_code = exports_t.og_product_code AND imports_t.og_period = exports_t.og_period AND imports_t.og_direction = 'imports' AND exports_t.og_direction = 'exports'
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
