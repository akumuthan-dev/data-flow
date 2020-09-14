import uuid
from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.dialects.postgresql import UUID

from dataflow.dags import _PipelineDAG
from dataflow.operators.db_tables import insert_csv_data_into_db
from dataflow.operators.ons import run_ipython_ons_extraction
from dataflow.transforms import transform_ons_marker_field
from dataflow.utils import TableConfig


class _ONSParserPipeline(_PipelineDAG):
    ons_script_dir = "need-to-override"

    def get_insert_data_callable(self):
        return insert_csv_data_into_db

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='run-ons-parser-script',
            python_callable=run_ipython_ons_extraction,
            provide_context=True,
            op_kwargs=dict(
                table_name=self.table_config.table_name, script_name=self.ons_script_dir
            ),
        )

    def get_source_data_modified_utc(self) -> datetime:
        from gssutils import Scraper
        import json

        with open(
            Path('.') / 'dataflow' / 'ons_scripts' / self.ons_script_dir / 'info.json'
        ) as info:
            distribution_info = json.load(info)['landingPage']

        if isinstance(distribution_info, list):
            issued_dates = set()
            for dist in distribution_info:
                scraper = Scraper(dist)
                issued_dates.add(scraper.dataset.issued)

            if len(issued_dates) != 1:
                raise ValueError(
                    "The files making up this dataset have different issue dates - refusing to continue with "
                    "inconsistent data."
                )

            issue_date = issued_dates.pop()
        else:
            scraper = Scraper(distribution_info)
            issue_date = scraper.dataset.issued

        return datetime(issue_date.year, issue_date.month, issue_date.day)

    def get_source_data_modified_utc_callable(self):
        return self.get_source_data_modified_utc


class ONSUKTotalTradeAllCountriesNSA(_ONSParserPipeline):
    start_date = datetime(2020, 4, 1)
    schedule_interval = "@monthly"

    ons_script_dir = 'uktotaltradeallcountriesnonseasonallyadjusted'

    table_config = TableConfig(
        table_name="ons__uk_total_trade_all_countries_nsa",
        transforms=[
            lambda record, table_config, contexts: {
                **record,
                "norm_period": record["Period"].split("/")[1],
                "norm_period_type": record["Period"].split("/")[0],
                "norm_total": int(float(record["Value"]))
                if "Value" in record and record["Value"] != ""
                else None,  # Convert redacted values ('') to Nones (NULL in DB)
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
            ("Geography Code", sa.Column("og_ons_iso_alpha_2_code", sa.String)),
            ("Geography Name", sa.Column("og_ons_region_name", sa.String)),
            ("Product", sa.Column("og_product_name", sa.String)),
            ("Period", sa.Column("og_period", sa.String)),
            ("norm_period", sa.Column("norm_period", sa.String)),
            ("norm_period_type", sa.Column("norm_period_type", sa.String)),
            ("Flow", sa.Column("og_direction", sa.String)),
            ("Value", sa.Column("og_total", sa.String)),
            ("norm_total", sa.Column("norm_total", sa.Integer)),
            ("Unit", sa.Column("og_unit", sa.String)),
            ("Marker", sa.Column("og_marker", sa.String)),
            ("norm_marker", sa.Column("norm_marker", sa.String)),
        ],
    )
