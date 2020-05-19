# This comment brought to you by the ** airflow DAG ** discovery process

from datetime import datetime


from dataflow.dags import _CSVPipelineDAG
from dataflow.dags.ons_parsing_pipelines import (
    ONSUKTradeInServicesByPartnerCountryNSAPipeline,
    ONSUKTotalTradeAllCountriesNSA,
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
        import_t.geography_code AS ons_geography_code,
        import_t.geography_name,
        CASE
            WHEN import_t.parent_geography_code = 'B5' THEN 'yes'
            ELSE 'no'
        END AS included_in_eu28,
        import_t.period,
        CASE
            WHEN char_length(import_t.period) = 4 THEN 'year'
            ELSE 'month'
        END AS period_type,
        import_t.total AS import,
        export_t.total AS export,
        export_t.total + import_t.total AS total_trade,
        export_t.total - import_t.total AS trade_balance,
        import_t.unit
    FROM
        {{ dependencies[0].table_config.table_name }} import_t INNER JOIN {{ dependencies[0].table_config.table_name }} export_t
        ON import_t.geography_code = export_t.geography_code AND import_t.period = export_t.period
    WHERE
        import_t.direction = 'Imports' AND
        export_t.direction = 'Exports'
    ORDER BY
        import_t.geography_name, import_t.period
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
WITH rolling_import_totals AS (SELECT geography_code,
                                      product_code,
                                      period,
                                      total,
                                      sum(total) over (PARTITION
                                          BY
                                          geography_code,
                                          product_code
                                          ORDER BY
                                              geography_code,
                                              product_code,
                                              period ASC rows between 3 preceding and current row) AS rolling_total
                               FROM public.ons_uk_trade_in_services_by_country_nsa
                               WHERE direction = 'imports'
                                 and period_type = 'quarter'
                               GROUP BY geography_code,
                                        product_code,
                                        period,
                                        total),
     rolling_export_totals AS (SELECT geography_code,
                                      product_code,
                                      period,
                                      total,
                                      sum(total) over (PARTITION
                                          BY
                                          geography_code ,
                                          product_code
                                          ORDER BY
                                              geography_code ,
                                              product_code ,
                                              period ASC rows between 3 preceding and current row) AS rolling_total
                               FROM public.ons_uk_trade_in_services_by_country_nsa
                               WHERE direction = 'exports'
                                 and period_type = 'quarter'
                               GROUP BY geography_code,
                                        product_code,
                                        period,
                                        total),
     imports_and_exports_with_rolling_totals AS (SELECT imports_t.geography_code,
                                                        imports_t.geography_name,
                                                        imports_t.product_code,
                                                        imports_t.product_name,
                                                        imports_t.period,
                                                        imports_t.period_type,
                                                        unnest(
                                                                array ['imports', 'exports', '4-quarter rolling imports total', '4-quarter rolling exports total'])         as "measure",
                                                        unnest(
                                                                array [imports_t.total, exports_t.total, rolling_imports_t.rolling_total, rolling_exports_t.rolling_total]) as "value",
                                                        imports_t.unit,
                                                        unnest(array [imports_t.marker, exports_t.marker, '', ''])                                                          as "marker"
                                                 FROM public.ons_uk_trade_in_services_by_country_nsa as imports_t
                                                          INNER JOIN
                                                      public.ons_uk_trade_in_services_by_country_nsa as exports_t
                                                      ON imports_t.geography_code = exports_t.geography_code
                                                          AND imports_t.product_code = exports_t.product_code
                                                          AND imports_t.period = exports_t.period
                                                          LEFT JOIN
                                                      rolling_import_totals rolling_imports_t
                                                      ON imports_t.geography_code = rolling_imports_t.geography_code
                                                          AND imports_t.product_code = rolling_imports_t.product_code
                                                          AND imports_t.period = rolling_imports_t.period
                                                          LEFT JOIN
                                                      rolling_export_totals rolling_exports_t
                                                      ON exports_t.geography_code = rolling_exports_t.geography_code
                                                          AND exports_t.product_code = rolling_exports_t.product_code
                                                          AND exports_t.period = rolling_exports_t.period
                                                 WHERE imports_t.direction = 'imports'
                                                   AND exports_t.direction = 'exports')
SELECT
    geography_code,
    geography_name,
    product_code,
    product_name,
    period,
    period_type,
    measure,
    value,
    unit,
    marker
FROM
    imports_and_exports_with_rolling_totals
WHERE
    (NOT (period_type = 'year' AND measure LIKE '4-quarter %')) AND NOT (measure LIKE '4-quarter %' AND (period < '2016-Q4'))
ORDER BY
    geography_name,
    product_code,
    period
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
WITH rolling_import_totals AS (SELECT geography_code,
                                      product_name,
                                      period,
                                      total,
                                      sum(total) over (PARTITION
                                          BY
                                          geography_code,
                                          product_name
                                          ORDER BY
                                              geography_code,
                                              product_name,
                                              period ASC rows between 3 preceding and current row) AS rolling_total
                               FROM public.ons_uk_total_trade_all_countries_nsa
                               WHERE direction = 'imports'
                                 and period_type = 'quarter'
                               GROUP BY geography_code,
                                        product_name,
                                        period,
                                        total),
     rolling_export_totals AS (SELECT geography_code,
                                      product_name,
                                      period,
                                      total,
                                      sum(total) over (PARTITION
                                          BY
                                          geography_code,
                                          product_name
                                          ORDER BY
                                              geography_code,
                                              product_name,
                                              period ASC rows between 3 preceding and current row) AS rolling_total
                               FROM public.ons_uk_total_trade_all_countries_nsa
                               WHERE direction = 'exports'
                                 and period_type = 'quarter'
                               GROUP BY geography_code,
                                        product_name,
                                        period,
                                        total),
     imports_and_exports_with_rolling_totals AS (SELECT imports_t.geography_code,
                                                        imports_t.geography_name,
                                                        imports_t.product_name,
                                                        imports_t.period,
                                                        imports_t.period_type,
                                                        unnest(
                                                                array ['imports', 'exports', 'trade balance', 'total trade', '4-quarter rolling imports total', '4-quarter rolling exports total'])                                               as "measure",
                                                        unnest(
                                                                array [imports_t.total, exports_t.total, exports_t.total - imports_t.total, exports_t.total + imports_t.total, rolling_imports_t.rolling_total, rolling_exports_t.rolling_total]) as "value",
                                                        imports_t.unit,
                                                        unnest(array [imports_t.marker, exports_t.marker, '', '', '', ''])                                                                                                                        as "marker"
                                                 FROM public.ons_uk_total_trade_all_countries_nsa as imports_t
                                                          INNER JOIN
                                                      public.ons_uk_total_trade_all_countries_nsa as exports_t
                                                      ON imports_t.geography_code = exports_t.geography_code
                                                          AND imports_t.product_name = exports_t.product_name
                                                          AND imports_t.period = exports_t.period
                                                          LEFT JOIN
                                                      rolling_import_totals rolling_imports_t
                                                      ON imports_t.geography_code = rolling_imports_t.geography_code
                                                          AND imports_t.product_name = rolling_imports_t.product_name
                                                          AND imports_t.period = rolling_imports_t.period
                                                          LEFT JOIN
                                                      rolling_export_totals rolling_exports_t
                                                      ON exports_t.geography_code = rolling_exports_t.geography_code
                                                          AND exports_t.product_name = rolling_exports_t.product_name
                                                          AND exports_t.period = rolling_exports_t.period
                                                 WHERE imports_t.direction = 'imports'
                                                   AND exports_t.direction = 'exports')
SELECT *
FROM imports_and_exports_with_rolling_totals
WHERE (NOT (period_type = 'year' AND measure LIKE '4-quarter %'))
  AND NOT (measure LIKE '4-quarter %' AND (period < '2016-Q4'))
ORDER BY geography_name,
         product_name,
         period;
"""
