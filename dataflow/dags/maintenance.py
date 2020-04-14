import re
from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

import sqlalchemy as sa

from dataflow import config
from dataflow.utils import logger, slack_alert


def cleanup_old_s3_files(*args, **kwargs):
    s3 = S3Hook("DEFAULT_S3")
    bucket = s3.get_bucket(config.S3_IMPORT_DATA_BUCKET)

    current_time = datetime.strptime(kwargs["ts_nodash"], "%Y%m%dT%H%M%S")
    logger.info(
        f"Retention period is {config.S3_RETENTION_PERIOD_DAYS} days before {current_time}"
    )

    pipelines = s3.list_prefixes(
        config.S3_IMPORT_DATA_BUCKET, prefix="import-data/", delimiter="/"
    )

    for pipeline in pipelines:
        run_ids = s3.list_prefixes(
            config.S3_IMPORT_DATA_BUCKET, prefix=pipeline, delimiter="/"
        )

        for run_id in run_ids:
            run_dt = datetime.strptime(run_id.split("/")[-2], "%Y%m%dT%H%M%S")
            if current_time - run_dt >= timedelta(days=config.S3_RETENTION_PERIOD_DAYS):
                logger.info(
                    f"Deleting {pipeline} run {run_id} ({run_dt}) older than retention period"
                )
                bucket.objects.filter(Prefix=run_id).delete()
            else:
                logger.info(f"Keeping {pipeline} run {run_id} ({run_dt})")


def cleanup_old_datasets_db_tables(*args, **kwargs):
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=config.DATASETS_DB_NAME).get_conn,
    )

    current_time = datetime.strptime(kwargs["ts_nodash"], "%Y%m%dT%H%M%S")
    logger.info(
        f"Retention period is {config.DB_TEMP_TABLE_RETENTION_PERIOD_DAYS} days before {current_time}"
    )

    with engine.begin() as conn:
        tables = [
            table
            for table, in conn.execute(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"
            )
        ]

        for table in tables:
            table_match = re.match(r"(.*)_(\d{8}t\d{6})(?:_swap)?", table)
            if not table_match:
                logger.info(f"Skipping {table}")
                continue

            table_dt = datetime.strptime(table_match.groups()[1], "%Y%m%dt%H%M%S")

            if current_time - table_dt >= timedelta(
                days=config.DB_TEMP_TABLE_RETENTION_PERIOD_DAYS
            ):
                if table_match.groups()[0] not in tables:
                    logger.warning(
                        f"Main table {table_match.groups()[0]} missing for {table}, skipping"
                    )
                else:
                    logger.info(
                        f"Deleting temporary table {table} ({table_dt}) older than retention period"
                    )
                    conn.execute(
                        "DROP TABLE {}".format(
                            engine.dialect.identifier_preparer.quote(table)
                        )
                    )
            else:
                logger.info(f"Keeping table {table}")


dag = DAG(
    "Maintenance",
    catchup=False,
    start_date=datetime(2020, 4, 7),
    schedule_interval="@daily",
    on_failure_callback=partial(slack_alert, success=False),
)


dag << PythonOperator(
    task_id='clean-up-s3', python_callable=cleanup_old_s3_files, provide_context=True
)


dag << PythonOperator(
    task_id='clean-up-datasets-db',
    python_callable=cleanup_old_datasets_db_tables,
    provide_context=True,
)
