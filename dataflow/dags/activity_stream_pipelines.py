from datetime import datetime, timedelta

import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dataflow.operators.db_tables import (
    create_temp_tables,
    drop_temp_tables,
    insert_data_into_db,
    swap_dataset_table,
)
from dataflow.operators.activity_stream import fetch_from_activity_stream


class BaseActivityStreamPipeline:
    target_db = "datasets_db"
    start_date = datetime(2019, 11, 5)
    end_date = None
    schedule_interval = "@daily"

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
        run_fetch_task_id = f"fetch-{self.name}-data"

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
                task_id=run_fetch_task_id,
                python_callable=fetch_from_activity_stream,
                provide_context=True,
                op_args=[self.index, self.query, run_fetch_task_id],
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
                op_args=[
                    self.target_db,
                    self.table,
                    self.field_mapping,
                    run_fetch_task_id,
                ],
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

        [_fetch, _create_tables] >> _insert_into_temp_table
        _insert_into_temp_table >> _swap_dataset_table >> _drop_tables

        return dag


class ERPPipeline(BaseActivityStreamPipeline):
    name = "erp"
    index = "objects"
    table_name = "erp_submissions"

    field_mapping = [
        ("id", sa.Column("id", sa.String, primary_key=True)),
        ("url", sa.Column("url", sa.String, primary_key=True)),
        (
            ("dit:directoryFormsApi:Submission:Data", "commodity", "commodity_code"),
            sa.Column("commodity_code", sa.ARRAY(sa.String)),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "commodity", "label"),
            sa.Column("commodity_label", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "company_name"),
            sa.Column("company_name", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "company_number"),
            sa.Column("company_number", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "company_type"),
            sa.Column("company_type", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "consumer_region"),
            sa.Column("consumer_region", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "consumer_type"),
            sa.Column("consumer_type", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "country"),
            sa.Column("country", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "email"),
            sa.Column("email", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "employees"),
            sa.Column("employees", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "employment_regions"),
            sa.Column("employment_regions", sa.ARRAY(sa.String)),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "equivalent_uk_goods"),
            sa.Column("equivalent_uk_goods", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "family_name"),
            sa.Column("family_name", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "given_name"),
            sa.Column("given_name", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_consumer_choice_changed"),
            sa.Column("has_consumer_choice_changed", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_consumer_price_changed"),
            sa.Column("has_consumer_price_changed", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_market_price_changed"),
            sa.Column("has_market_price_changed", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_market_size_changed"),
            sa.Column("has_market_size_changed", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_other_changes"),
            sa.Column("has_other_changes", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_price_changed"),
            sa.Column("has_price_changed", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "has_volume_changed"),
            sa.Column("has_volume_changed", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "import_countries"),
            sa.Column("import_countries", sa.ARRAY(sa.String)),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "income_bracket"),
            sa.Column("income_bracket", sa.ARRAY(sa.String)),
        ),
        (
            (
                "dit:directoryFormsApi:Submission:Data",
                "imported_goods_makes_something_else",
            ),
            sa.Column("imported_goods_makes_something_else", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "market_size_known"),
            sa.Column("market_size_known", sa.Boolean),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "other_information"),
            sa.Column("other_information", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "production_cost_percentage"),
            sa.Column("production_cost_percentage", sa.Integer),
        ),
        (
            (
                "dit:directoryFormsApi:Submission:Data",
                "quarter_four_2018_sales_revenue",
            ),
            sa.Column("quarter_four_2018_sales_revenue", sa.Integer),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "quarter_four_2018_sales_volume"),
            sa.Column("quarter_four_2018_sales_volume", sa.Integer),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "quarter_one_2019_sales_revenue"),
            sa.Column("quarter_one_2019_sales_revenue", sa.Integer),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "quarter_one_2019_sales_volume"),
            sa.Column("quarter_one_2019_sales_volume", sa.Integer),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "quarter_two_2019_sales_revenue"),
            sa.Column("quarter_two_2019_sales_revenue", sa.Integer),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "quarter_two_2019_sales_volume"),
            sa.Column("quarter_two_2019_sales_volume", sa.Integer),
        ),
        (
            (
                "dit:directoryFormsApi:Submission:Data",
                "quarter_three_2019_sales_revenue",
            ),
            sa.Column("quarter_three_2019_sales_revenue", sa.Integer),
        ),
        (
            (
                "dit:directoryFormsApi:Submission:Data",
                "quarter_three_2019_sales_volume",
            ),
            sa.Column("quarter_three_2019_sales_volume", sa.Integer),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "sales_volume_unit"),
            sa.Column("sales_volume_unit", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "sector"),
            sa.Column("sector", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "tariff_quota"),
            sa.Column("tariff_quota", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "tariff_rate"),
            sa.Column("tariff_rate", sa.String),
        ),
        (
            ("dit:directoryFormsApi:Submission:Data", "turnover"),
            sa.Column("turnover", sa.String),
        ),
    ]

    query = {
        "bool": {
            "filter": [
                {
                    "term": {
                        "attributedTo.id": "dit:directoryFormsApi:SubmissionType:ExceptionalReviewProcedure"
                    }
                },
                {
                    "term": {
                        "attributedTo.type": "dit:directoryFormsApi:SubmissionAction:zendesk"
                    }
                },
            ]
        }
    }


for pipeline in BaseActivityStreamPipeline.__subclasses__():
    globals()[pipeline.__name__ + "__dag"] = pipeline().get_dag()
