# This comment brought to you by the ** airflow DAG ** discovery process

from datetime import datetime


from dataflow.dags import _CSVPipelineDAG


class ONSUKSATradeInGoodsCSV(_CSVPipelineDAG):
    # We trigger this DAG manually from the related polling pipeline. We do this because the polling pipeline will often
    # sit for 10+ hours and then skip the remaining tasks because it hasn't found new data. The way `dependencies`
    # work is to look for a `success` state on the `swap-dataset-tables` task, which doesn't happen if the polling
    # pipeline skips its workload. That results in this pipeline's external task sensor failing and eventually
    # throwing an error (which gets reported by sentry) that we don't care about.
    # Triggering this DAG manually will only work while there is a single parent pipeline. If that changes, we'll need
    # to switch back to using `dependencies` and come up with an alternative solution to the polling timeout problem.
    schedule_interval = None
    # dependencies = [ONSUKSATradeInGoodsPollingPipeline]

    start_date = datetime(2020, 4, 1)
    catchup = False
    static = True

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
                ORDER BY ons_region_name, period
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
                ORDER BY i.ons_region_name, i.period
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
                ORDER BY i.ons_region_name, i.period
                ROWS between 11 preceding and current row)
    )
    ORDER BY ons_region_name, period, direction
) AS query WHERE (period > '1998-11') OR period_type != '12 months ending'
    """


class ONSUKTradeInServicesByPartnerCountryNSACSV(_CSVPipelineDAG):
    # We trigger this DAG manually from the related polling pipeline. We do this because the polling pipeline will often
    # sit for 10+ hours and then skip the remaining tasks because it hasn't found new data. The way `dependencies`
    # work is to look for a `success` state on the `swap-dataset-tables` task, which doesn't happen if the polling
    # pipeline skips its workload. That results in this pipeline's external task sensor failing and eventually
    # throwing an error (which gets reported by sentry) that we don't care about.
    # Triggering this DAG manually will only work while there is a single parent pipeline. If that changes, we'll need
    # to switch back to using `dependencies` and come up with an alternative solution to the polling timeout problem.
    schedule_interval = None
    # dependencies = [ONSUKTradeInServicesByPartnerCountryNSAPollingPipeline]

    start_date = datetime(2020, 4, 1)
    catchup = False
    static = True

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
        value as trade_value,
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
            sum(value) over w AS trade_value,
            unit,
            'derived' as marker
        FROM ons.uk_trade_in_services_by_country_nsa
        WHERE period_type = 'quarter'
        GROUP BY ons_iso_alpha_2_code, ons_region_name, period, direction, product_code, product_name, value, unit, marker
        WINDOW w AS (
            PARTITION BY ons_iso_alpha_2_code, direction, product_code
            ORDER BY ons_iso_alpha_2_code, direction, product_code, period
            ROWS between 3 preceding and current row
        )
        ORDER BY ons_region_name, period, direction, product_code
    )
    ORDER BY ons_region_name, period, direction, product_code
) AS query WHERE period_type != '4 quarters ending' OR period >= '2016-Q4'
"""


class ONSUKTotalTradeAllCountriesNSACSVPipeline(_CSVPipelineDAG):
    # We trigger this DAG manually from the related polling pipeline. We do this because the polling pipeline will often
    # sit for 10+ hours and then skip the remaining tasks because it hasn't found new data. The way `dependencies`
    # work is to look for a `success` state on the `swap-dataset-tables` task, which doesn't happen if the polling
    # pipeline skips its workload. That results in this pipeline's external task sensor failing and eventually
    # throwing an error (which gets reported by sentry) that we don't care about.
    # Triggering this DAG manually will only work while there is a single parent pipeline. If that changes, we'll need
    # to switch back to using `dependencies` and come up with an alternative solution to the polling timeout problem.
    schedule_interval = None
    # dependencies = [ONSUKTotalTradeAllCountriesNSAPollingPipeline]

    start_date = datetime(2020, 4, 1)

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
    value AS trade_value,
    unit,
    marker
FROM (
    SELECT
        ons_iso_alpha_2_code,
        ons_region_name,
        period,
        period_type,
        direction,
        product_name,
        value,
        unit,
        marker
    FROM ons.uk_total_trade_all_countries_nsa
    UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            i.period_type,
            'total trade' AS direction,
            i.product_name,
            e.value + i.value AS value,
            i.unit,
            'derived' AS marker
        FROM ons.uk_total_trade_all_countries_nsa e inner join ons.uk_total_trade_all_countries_nsa i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports'
    ) UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            i.period_type,
            'trade balance' AS direction,
            i.product_name,
            e.value - i.value AS value,
            i.unit,
            'derived' AS marker
        FROM ons.uk_total_trade_all_countries_nsa e inner join ons.uk_total_trade_all_countries_nsa i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports'
    ) UNION (
        SELECT
            ons_iso_alpha_2_code,
            ons_region_name,
            period,
            '4 quarters ending' AS period_type,
            direction,
            product_name,
            sum(value) over w AS value,
            unit,
            'derived' AS marker
        FROM ons.uk_total_trade_all_countries_nsa
        WHERE period_type = 'quarter'
        GROUP BY ons_iso_alpha_2_code, uk_total_trade_all_countries_nsa.ons_region_name, period, direction, product_name, value, unit, marker
        WINDOW w AS (
            PARTITION BY ons_iso_alpha_2_code, direction, product_name
            ORDER BY ons_iso_alpha_2_code, direction, product_name, period
            ROWS between 3 preceding and current row
        )
    ) UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            '4 quarters ending' AS period_type,
            'trade balance' AS direction,
            i.product_name,
            sum(e.value - i.value) over w AS value,
            i.unit,
            'derived' AS marker
        FROM ons.uk_total_trade_all_countries_nsa e inner join ons.uk_total_trade_all_countries_nsa i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports' AND i.period_type = 'quarter'
        GROUP BY i.ons_iso_alpha_2_code, i.ons_region_name, i.period, i.direction, i.product_name, e.value, i.unit, i.value
        WINDOW w AS (
                PARTITION BY i.ons_region_name, i.direction, i.product_name
                ORDER BY i.ons_region_name, i.direction, i.product_name, i.period
                ROWS between 3 preceding and current row)
    ) UNION (
        SELECT
            i.ons_iso_alpha_2_code,
            i.ons_region_name,
            i.period,
            '4 quarters ending' AS period_type,
            'total trade' AS direction,
            i.product_name,
            sum(e.value + i.value) over w AS value,
            i.unit,
            'derived' AS marker
        FROM ons.uk_total_trade_all_countries_nsa e inner join ons.uk_total_trade_all_countries_nsa i
        ON i.ons_iso_alpha_2_code = e.ons_iso_alpha_2_code AND i.period = e.period AND i.product_name = e.product_name
        WHERE e.direction = 'exports' AND i.direction = 'imports' AND i.period_type = 'quarter'
        GROUP BY i.ons_iso_alpha_2_code, i.ons_region_name, i.period, i.direction, i.product_name, e.value, i.unit, i.value
        WINDOW w AS (
                PARTITION BY i.ons_region_name, i.direction, i.product_name
                ORDER BY i.ons_region_name, i.direction, i.product_name, i.period
                ROWS between 3 preceding and current row)
    )
    ORDER BY ons_region_name, period, period_type, direction, product_name
) AS query WHERE period_type != '4 quarters ending' OR period >= '2016-Q4'
"""


class ONSUKTradeInGoodsByCountryAndCommodityCSVPipeline(_CSVPipelineDAG):
    # We trigger this DAG manually from the related polling pipeline. We do this because the polling pipeline will often
    # sit for 10+ hours and then skip the remaining tasks because it hasn't found new data. The way `dependencies`
    # work is to look for a `success` state on the `swap-dataset-tables` task, which doesn't happen if the polling
    # pipeline skips its workload. That results in this pipeline's external task sensor failing and eventually
    # throwing an error (which gets reported by sentry) that we don't care about.
    # Triggering this DAG manually will only work while there is a single parent pipeline. If that changes, we'll need
    # to switch back to using `dependencies` and come up with an alternative solution to the polling timeout problem.
    schedule_interval = None
    # dependencies = [ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline]

    start_date = datetime(2020, 4, 1)

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
           unnest(array[imports_t.value, exports_t.value, exports_t.value - imports_t.value, exports_t.value + imports_t.value]) AS value,
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
         sum(value) over (PARTITION
             BY
             ons_iso_alpha_2_code,
             product_code,
             direction
             ORDER BY
                 ons_iso_alpha_2_code,
                 product_code,
                 direction,
                 period rows between 11 preceding and current row) AS value,
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
              value,
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
    value AS trade_value,
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
