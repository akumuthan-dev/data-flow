# This comment brought to you by the ** airflow DAG ** discovery process

from datetime import datetime

from dataflow.dags import _CSVPipelineDAG
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
