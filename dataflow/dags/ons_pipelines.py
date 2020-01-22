from datetime import datetime, timedelta
from typing import Optional

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow import config
from dataflow.operators.db_tables import (
    check_table_data,
    create_temp_tables,
    drop_temp_tables,
    insert_data_into_db,
    swap_dataset_table,
)
from dataflow.operators.ons import fetch_from_ons_sparql


class BaseONSPipeline:
    target_db = config.DATASETS_DB_NAME
    start_date = datetime(2019, 11, 5)
    end_date = None
    schedule_interval = "@daily"

    index_query: Optional[str] = None

    @property
    def table(self):
        if not hasattr(self, "_table"):
            meta = sa.MetaData()
            self._table = sa.Table(
                self.table_name,
                meta,
                *[column.copy() for _, column in self.field_mapping],
            )

        return self._table

    def get_dag(self):
        with DAG(
            self.__class__.__name__,
            catchup=False,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 0,
                "retry_delay": timedelta(minutes=5),
            },
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
            max_active_runs=1,
        ) as dag:
            _fetch = PythonOperator(
                task_id="fetch-from-ons-sparql",
                python_callable=fetch_from_ons_sparql,
                provide_context=True,
                op_args=[self.table_name, self.query, self.index_query],
            )

            _create_tables = PythonOperator(
                task_id="create-temp-tables",
                python_callable=create_temp_tables,
                provide_context=True,
                op_args=[self.target_db, self.table],
            )

            _insert_into_temp_table = PythonOperator(
                task_id="insert-into-temp-table",
                python_callable=insert_data_into_db,
                provide_context=True,
                op_args=[self.target_db, self.table, self.field_mapping],
            )

            _check_tables = PythonOperator(
                task_id="check-temp-table-data",
                python_callable=check_table_data,
                provide_context=True,
                op_args=[self.target_db, self.table],
            )

            _swap_dataset_table = PythonOperator(
                task_id="swap-dataset-table",
                python_callable=swap_dataset_table,
                provide_context=True,
                op_args=[self.target_db, self.table],
            )

            _drop_tables = PythonOperator(
                task_id="drop-temp-tables",
                python_callable=drop_temp_tables,
                provide_context=True,
                trigger_rule="all_done",
                op_args=[self.target_db, self.table],
            )

        (
            [_fetch, _create_tables]
            >> _insert_into_temp_table
            >> _check_tables
            >> _swap_dataset_table
            >> _drop_tables
        )

        return dag


class ONSUKSATradeInGoodsPipeline(BaseONSPipeline):
    table_name = "ons_uk_sa_trade_in_goods"

    field_mapping = [
        (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
        (("period", "value"), sa.Column("period", sa.String)),
        (("geography_name", "value"), sa.Column("geography_name", sa.String)),
        (("direction", "value"), sa.Column("direction", sa.String)),
        (("total", "value"), sa.Column("total", sa.Numeric(asdecimal=True))),
        (("unit", "value"), sa.Column("unit", sa.String)),
    ]

    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?period ?geography_name ?direction (xsd:decimal(?gbp_total) AS ?total) ?unit WHERE {
        ?s <http://purl.org/linked-data/cube#dataSet> <http://gss-data.org.uk/data/gss_data/trade/ons-uk-sa-trade-in-goods> ;
            <http://gss-data.org.uk/def/dimension/flow> ?direction_s ;
            <http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure> ?unit_s ;
            <http://gss-data.org.uk/def/measure/gbp-total> ?gbp_total ;
            <http://gss-data.org.uk/def/dimension/trade-partner-geography> ?geography_s ;
            <http://purl.org/linked-data/sdmx/2009/dimension#refPeriod> ?period_s .

    ?period_s <http://www.w3.org/2000/01/rdf-schema#label> ?period .
    ?direction_s <http://www.w3.org/2000/01/rdf-schema#label> ?direction .
    ?geography_s <http://www.w3.org/2000/01/rdf-schema#label> ?geography_name .
    ?geography_s <http://www.w3.org/2004/02/skos/core#notation> ?geography_code .
    ?unit_s <http://www.w3.org/2000/01/rdf-schema#label> ?unit .

    } ORDER BY ?period ?geography_s
    """


class ONSUKTradeInGoodsPipeline(BaseONSPipeline):
    table_name = "ons_uk_trade_in_goods"
    schedule_interval = "@weekly"

    field_mapping = [
        (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
        (("period", "value"), sa.Column("period", sa.String)),
        (("geography_name", "value"), sa.Column("geography_name", sa.String)),
        (("product", "value"), sa.Column("product", sa.String)),
        (("direction", "value"), sa.Column("direction", sa.String)),
        (("total", "value"), sa.Column("total", sa.Numeric(asdecimal=True))),
        (("unit", "value"), sa.Column("unit", sa.String)),
    ]

    index_query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX pmdqb: <http://publishmydata.com/def/qb/>

    SELECT ?compvalue ?label WHERE {
    BIND(<http://gss-data.org.uk/data/gss_data/trade/ons-trade-in-goods> AS ?dataset)
    BIND(<http://purl.org/linked-data/sdmx/2009/dimension#refPeriod> AS ?component)
    ?dataset qb:structure/qb:component ?compspec .

    ?compspec ?comp_type ?component ;
                pmdqb:codesUsed / skos:member ?compvalue .

    ?compvalue rdfs:label ?label .

    } ORDER BY ?compvalue
    """

    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    {% raw %}

    SELECT ?period ?geography_name ?product ?direction (xsd:decimal(?gbp_total) AS ?total) ?unit WHERE {{

        BIND(<{compvalue[value]}> AS ?period_s)

        ?s <http://purl.org/linked-data/cube#dataSet> <http://gss-data.org.uk/data/gss_data/trade/ons-trade-in-goods> ;
        <http://purl.org/linked-data/sdmx/2009/dimension#refPeriod> ?period_s ;
        <http://gss-data.org.uk/def/dimension/trade-partner-geography> ?geography_s ;
        <http://gss-data.org.uk/def/dimension/product> ?product_s ;
        <http://gss-data.org.uk/def/dimension/flow> ?direction_s ;
        <http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure> ?unit_s ;
        <http://gss-data.org.uk/def/measure/gbp-total> ?gbp_total .

        ?period_s <http://www.w3.org/2000/01/rdf-schema#label> ?period .
        ?direction_s <http://www.w3.org/2000/01/rdf-schema#label> ?direction .
        ?geography_s <http://www.w3.org/2000/01/rdf-schema#label> ?geography_name .
        ?geography_s <http://www.w3.org/2004/02/skos/core#notation> ?geography_code .
        ?unit_s <http://www.w3.org/2000/01/rdf-schema#label> ?unit .
        ?product_s <http://www.w3.org/2000/01/rdf-schema#label> ?product .
    }} ORDER BY ?geography_s ?product_s

    {% endraw %}
    """


class ONSUKTradeInGoodsByCommodityPipeline(BaseONSPipeline):
    table_name = "ons_uk_trade_in_goods_by_commodity"

    field_mapping = [
        (None, sa.Column("id", sa.Integer, primary_key=True, autoincrement=True)),
        (("period", "value"), sa.Column("period", sa.String)),
        (("geography_name", "value"), sa.Column("geography_name", sa.String)),
        (("direction", "value"), sa.Column("direction", sa.String)),
        (("total", "value"), sa.Column("total", sa.Numeric(asdecimal=True))),
        (("unit", "value"), sa.Column("unit", sa.String)),
        (("sic_label", "value"), sa.Column("sector", sa.String)),
        (("product_label", "value"), sa.Column("product", sa.String)),
    ]

    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?geography_name ?sic_label ?product_label ?period ?direction (xsd:decimal(?gbp_total) AS ?total) ?unit WHERE {
        ?s <http://purl.org/linked-data/cube#dataSet> <http://gss-data.org.uk/data/gss_data/trade/ons-uk-trade-in-goods-by-industry-country-and-commodity> ;
        <http://gss-data.org.uk/def/dimension/product> ?product_s ;
        <http://gss-data.org.uk/def/dimension/sic-industry> ?sic_industry_s ;
            <http://gss-data.org.uk/def/dimension/flow> ?direction_s ;
            <http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure> ?unit_s ;
            <http://gss-data.org.uk/def/measure/gbp-total> ?gbp_total ;
            <http://gss-data.org.uk/def/dimension/trade-partner-geography> ?geography_s ;
            <http://purl.org/linked-data/sdmx/2009/dimension#refPeriod> ?period_s .

    ?period_s <http://www.w3.org/2000/01/rdf-schema#label> ?period .
    ?direction_s <http://www.w3.org/2000/01/rdf-schema#label> ?direction .
    ?geography_s <http://www.w3.org/2000/01/rdf-schema#label> ?geography_name .
    ?geography_s <http://www.w3.org/2004/02/skos/core#notation> ?geography_code .
    ?unit_s <http://www.w3.org/2000/01/rdf-schema#label> ?unit .
    ?sic_industry_s <http://www.w3.org/2000/01/rdf-schema#label> ?sic_label .
    ?sic_industry_s <http://business.data.gov.uk/companies/def/sic-2007/sicNotation> ?sic_code .
    ?product_s <http://www.w3.org/2000/01/rdf-schema#label> ?product_label .

    } ORDER BY ?product_s ?sic_industry_s ?geography_s ?period_s
    """


for pipeline in BaseONSPipeline.__subclasses__():
    globals()[pipeline.__name__ + "__dag"] = pipeline().get_dag()
