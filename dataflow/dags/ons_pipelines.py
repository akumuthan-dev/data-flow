from typing import Optional


import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator

from dataflow.dags import _PipelineDAG
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


class ONSUKSATradeInGoodsPipeline(_ONSPipeline):
    table_config = TableConfig(
        table_name="ons_uk_sa_trade_in_goods",
        field_mapping=[
            (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
            (("period", "value"), sa.Column("period", sa.String)),
            (("geography_name", "value"), sa.Column("geography_name", sa.String)),
            (("geography_code", "value"), sa.Column("geography_code", sa.String)),
            (("direction", "value"), sa.Column("direction", sa.String)),
            (("total", "value"), sa.Column("total", sa.Numeric)),
            (("unit", "value"), sa.Column("unit", sa.String)),
        ],
    )

    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?period ?geography_name ?geography_code ?direction (xsd:decimal(?gbp_total) AS ?total) ?unit
    WHERE {
        ?s <http://purl.org/linked-data/cube#dataSet> <http://gss-data.org.uk/data/gss_data/trade/ons-uk-sa-trade-in-goods> ;
            <http://gss-data.org.uk/def/dimension/flow-directions> ?direction_s ;
            <http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure> ?unit_s ;
            <http://gss-data.org.uk/def/measure/gbp-total> ?gbp_total ;
            <http://gss-data.org.uk/def/dimension/ons-partner-geography> ?geography_s ;
            <http://purl.org/linked-data/sdmx/2009/dimension#refPeriod> ?period_s .

        ?period_s <http://www.w3.org/2000/01/rdf-schema#label> ?period .
        ?direction_s <http://www.w3.org/2000/01/rdf-schema#label> ?direction .
        ?geography_s <http://www.w3.org/2000/01/rdf-schema#label> ?geography_name .
        ?geography_s <http://www.w3.org/2004/02/skos/core#notation> ?geography_code .
        ?unit_s <http://www.w3.org/2000/01/rdf-schema#label> ?unit .

    }
    ORDER BY ?period ?geography_s
    """
