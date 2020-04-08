import csv
import tempfile

import sqlalchemy as sa
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from dataflow import config
from dataflow.utils import logger


def create_csv(
    target_db: str, base_file_name: str, timestamp_output: bool, query: str, **kwargs
):
    """
    Given a db, view name and a query create a csv file and upload it to s3.
    """
    if timestamp_output:
        end_date = kwargs.get('run_date', kwargs.get('next_execution_date'))
        file_name = f'{base_file_name}_{end_date.strftime("%Y_%m_%d")}.csv'
    else:
        file_name = f'{base_file_name}.csv'

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
        echo=config.DEBUG,
    )
    row_count = 0
    run_date = kwargs.get('run_date', kwargs.get('execution_date'))
    with engine.begin() as conn:
        result = conn.execution_options(stream_results=True).execute(
            sa.text(query), run_date=run_date.date()
        )
        with tempfile.NamedTemporaryFile('w', encoding='utf8') as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(result.keys())
            while True:
                chunk = result.fetchmany(1000)
                if not chunk:
                    break
                row_count += len(chunk)
                for row in chunk:
                    writer.writerow(row)
            fh.flush()

            s3_client = S3Hook('DATA_WORKSPACE_S3')
            s3_output_path = f's3://csv-pipelines/{base_file_name}/{file_name}'
            s3_client.load_file(
                fh.name,
                s3_output_path,
                bucket_name=config.DATA_WORKSPACE_S3_BUCKET,
                replace=True,
            )
            logger.info(f'Wrote {row_count} rows to file {s3_output_path}')
