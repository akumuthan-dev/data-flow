import os
import subprocess
import threading
from urllib.parse import urlparse

import boto3

from dataflow.config import (
    UK_TARIFF_BUCKET_NAME,
    UK_TARIFF_BUCKET_AWS_ACCESS_KEY_ID,
    UK_TARIFF_BUCKET_AWS_SECRET_ACCESS_KEY,
)
from dataflow.utils import logger


def ingest_uk_tariff_to_temporary_db(**kwargs):
    # The UK Tariff comes as a pgdump, to be ingested into the DB into its own, temporary, database
    # using pg_restore. pg_restore is an application run in a separate process. While technically,
    # we could use Python to drop/create the database, pg_restore is (usually) part of the same
    # package, and has similar command line arguments to psql, so we use the same pattern for both
    # both

    ######

    logger.info('Extracting credentials from environent')

    parsed_db_uri = urlparse(os.environ['AIRFLOW_CONN_DATASETS_DB'])
    host, port, dbname, user, password = (
        parsed_db_uri.hostname,
        parsed_db_uri.port or '5432',
        parsed_db_uri.path.strip('/'),
        parsed_db_uri.username,
        parsed_db_uri.password or '',  # No password locally
    )
    logger.info('host: %s, port: %s, dbname: %s, user:%s', host, port, dbname, user)

    ######

    logger.info('(Re)creating temporary database')

    completed_recreate_process = subprocess.run(
        [
            "psql",
            "-h",
            host,
            "-p",
            port,
            "-U",
            user,
            "-d",
            dbname,
            "-c",
            "SELECT pg_terminate_backend(pg_stat_activity.pid) "
            "FROM pg_stat_activity "
            "WHERE pg_stat_activity.datname = 'temporary__uk_tariff'"
            "AND pid <> pg_backend_pid();",
            "-c",
            "DROP DATABASE IF EXISTS temporary__uk_tariff;",
            "-c",
            "CREATE DATABASE temporary__uk_tariff;",
        ],
        bufsize=1048576,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, 'PGPASSWORD': password},
    )
    for line in completed_recreate_process.stdout.splitlines():
        logger.info('psql >>>> ' + line.rstrip().decode('utf-8'))
    completed_recreate_process.check_returncode()

    ######

    logger.info('Finding latest key')

    s3_client = boto3.client(
        's3',
        aws_access_key_id=UK_TARIFF_BUCKET_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=UK_TARIFF_BUCKET_AWS_SECRET_ACCESS_KEY,
    )

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterable = paginator.paginate(
        Bucket=UK_TARIFF_BUCKET_NAME, Prefix='uk/', Delimiter='/'
    )

    latest_key = None
    for page in page_iterable:
        for item in page.get('Contents', []):
            latest_key = item['Key']
    if latest_key is None:
        raise Exception('No keys found in bucket')

    logger.info('Latest key %s', latest_key)

    ######

    logger.info('Fetching object and piping to pg_restore')

    s3_response = s3_client.get_object(Bucket=UK_TARIFF_BUCKET_NAME, Key=latest_key)

    import_process = subprocess.Popen(
        [
            "pg_restore",
            "-h",
            host,
            "-p",
            port,
            "-d",
            "temporary__uk_tariff",
            "-U",
            user,
        ],
        bufsize=1048576,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        env={**os.environ, 'PGPASSWORD': password},
    )

    def log_stdout(proc):
        for line in iter(import_process.stdout.readline, b''):
            logger.info(line.rstrip().decode('utf-8'))

    threading.Thread(target=log_stdout).start()

    for chunk in s3_response['Body'].iter_chunks(65536):
        import_process.stdin.write(chunk)

    import_process.stdin.close()
    import_process.wait()

    # Unfortunately, there are lots of "errors" in the restore, so pg_restore exits with a
    # non-zero code, but it is a success in terms of the import of data
    logger.info('pg_restore existed wth code %s', import_process.returncode)

    logger.info('Done')
