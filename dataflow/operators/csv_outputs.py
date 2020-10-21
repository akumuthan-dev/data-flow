import csv
import tempfile
from io import StringIO

import sqlalchemy as sa
import zipstream
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from dataflow import config
from dataflow.utils import logger


def create_csv(
    target_db: str,
    base_file_name: str,
    timestamp_output: bool,
    query: str,
    **kwargs,
):
    """
    Given a db, view name and a query create a csv file and upload it to s3.
    """
    if timestamp_output:
        file_name = (
            f'{base_file_name}-{kwargs["next_execution_date"].strftime("%Y-%m-%d")}.csv'
        )
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

            logger.info(f'Wrote {row_count} rows to file {file_name}')

            s3_client = S3Hook('DATA_WORKSPACE_S3')
            s3_output_path = f's3://csv-pipelines/{base_file_name}/{file_name}'
            s3_client.load_file(
                fh.name,
                s3_output_path,
                bucket_name=config.DATA_WORKSPACE_S3_BUCKET,
                replace=True,
            )

            logger.info(f"Uploaded {file_name} to {s3_output_path}")


def create_compressed_csv(
    target_db: str,
    base_file_name: str,
    timestamp_output: bool,
    query: str,
    **kwargs,
):
    """
    Given a db, view name and a query create a csv file and upload it to s3.
    """
    if timestamp_output:
        file_name = (
            f'{base_file_name}-{kwargs["next_execution_date"].strftime("%Y-%m-%d")}.csv'
        )
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

        def iter_results_as_csv_lines():
            nonlocal row_count
            s = StringIO()

            writer = csv.writer(s, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(result.keys())

            while True:
                chunk = result.fetchmany(1000)
                if not chunk:
                    break

                row_count += len(chunk)
                for row in chunk:
                    writer.writerow(row)

                yield s.getvalue().encode('utf8')
                s.truncate(0)
                s.seek(0)

        zip_streamer = zipstream.ZipFile(compression=zipstream.ZIP_DEFLATED)
        zip_streamer.write_iter(file_name, iter_results_as_csv_lines())

        with tempfile.NamedTemporaryFile("wb") as fh:
            logger.info(f"Compressing data to {fh.name}")

            for data in zip_streamer:
                fh.write(data)
            fh.flush()

            logger.info(f'Wrote {row_count} rows to file {file_name} in {fh.name}')

            s3_client = S3Hook('DATA_WORKSPACE_S3')
            s3_output_path = f's3://csv-pipelines/{base_file_name}/{file_name}.zip'
            s3_client.load_file(
                fh.name,
                s3_output_path,
                bucket_name=config.DATA_WORKSPACE_S3_BUCKET,
                replace=True,
            )

            logger.info(f"Uploaded {file_name} to {s3_output_path}")
