import uuid
from datetime import datetime
from typing import Optional


import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
from dataflow.dags.base import _FastPollingPipeline
from dataflow.ons_scripts.uktradecountrybycommodity.main import (
    get_source_data_modified_date,
    get_data,
)
from dataflow.operators.ons import fetch_from_ons_sparql
from dataflow.transforms import transform_ons_marker_field
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


class ONSUKSATradeInGoodsPipeline(_ONSPipeline):
    table_config = TableConfig(
        table_name="ons__uk_sa_trade_in_goods",
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                "norm_period_type": "year"
                if len(record["period"]["value"]) == 4
                else "month",
                "norm_total": int(float(record["total"]["value"]))
                if "total" in record
                else None,
            },
            transform_ons_marker_field,
        ],
        field_mapping=[
            (
                None,
                sa.Column(
                    "id", UUID, primary_key=True, default=lambda: str(uuid.uuid4())
                ),
            ),
            (None, sa.Column("import_time", sa.DateTime, default=datetime.utcnow)),
            (
                ("geography_code", "value"),
                sa.Column("og_ons_iso_alpha_2_code", sa.String),
            ),
            (("geography_name", "value"), sa.Column("og_ons_region_name", sa.String)),
            (("period", "value"), sa.Column("og_period", sa.String)),
            (("norm_period_type"), sa.Column("norm_period_type", sa.String)),
            (("direction", "value"), sa.Column("og_direction", sa.String)),
            (("total", "value"), sa.Column("og_total", sa.String)),
            ("norm_total", sa.Column("norm_total", sa.Integer)),
            (("unit", "value"), sa.Column("og_unit", sa.String)),
            (("marker", "value"), sa.Column("og_marker", sa.String)),
            ("norm_marker", sa.Column("norm_marker", sa.String)),
        ],
    )

    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?period ?geography_name ?geography_code ?direction (xsd:decimal(?gbp_total) AS ?total) ?unit ?marker
    WHERE {
        ?s <http://purl.org/linked-data/cube#dataSet> <http://gss-data.org.uk/data/gss_data/trade/ons-uk-sa-trade-in-goods> ;
            <http://gss-data.org.uk/def/dimension/flow-directions> ?direction_s ;
            <http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure> ?unit_s ;
            <http://gss-data.org.uk/def/dimension/ons-partner-geography> ?geography_s ;
            <http://purl.org/linked-data/sdmx/2009/dimension#refPeriod> ?period_s .

        ?period_s <http://www.w3.org/2000/01/rdf-schema#label> ?period .
        ?direction_s <http://www.w3.org/2000/01/rdf-schema#label> ?direction .
        ?geography_s <http://www.w3.org/2000/01/rdf-schema#label> ?geography_name .
        ?geography_s <http://www.w3.org/2004/02/skos/core#notation> ?geography_code .
        ?unit_s <http://www.w3.org/2000/01/rdf-schema#label> ?unit .

        OPTIONAL {
            ?s <http://gss-data.org.uk/def/measure/gbp-total> ?gbp_total .
        }

        OPTIONAL {
            ?s <http://purl.org/linked-data/sdmx/2009/attribute#obsStatus> ?marker_s .
            ?marker_s <http://www.w3.org/2004/02/skos/core#notation> ?marker .
        }

    }
    ORDER BY ?period ?geography_s
    """


class ONSUKTradeInGoodsByCountryAndCommodityPollingPipeline(_FastPollingPipeline):
    date_checker = get_source_data_modified_date
    data_getter = get_data
    table_config = TableConfig(
        schema='ons',
        table_name='uk_trade_in_goods_by_country_and_commodity',
        field_mapping=[],
    )
    queue = 'high-memory-usage'
