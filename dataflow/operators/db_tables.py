import csv
import itertools
import warnings
from io import StringIO
from typing import Tuple, Dict, Optional

import sqlalchemy as sa
from airflow.hooks.postgres_hook import PostgresHook

from dataflow import config
from dataflow.utils import (
    get_nested_key,
    get_temp_table,
    S3Data,
    TableConfig,
    SingleTableFieldMapping,
    logger,
)


class MissingDataError(ValueError):
    pass


class UnusedColumnError(ValueError):
    pass


def create_temp_tables(target_db: str, *tables: sa.Table, **kwargs):
    """
    Create a temporary table for the current DAG run for each of the given dataset
    tables.


    Table names are unique for each DAG run and use target table name as a prefix
    and current DAG execution timestamp as a suffix.

    """

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    with engine.begin() as conn:
        conn.execute("SET statement_timeout = 600000")
        for table in tables:
            table = get_temp_table(table, kwargs["ts_nodash"])
            logger.info(f"Creating schema {table.schema} if not exists")
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {table.schema}")
            logger.info(f"Creating {table.name}")
            table.create(conn, checkfirst=True)


def _get_data_to_insert(field_mapping: SingleTableFieldMapping, record: Dict):
    try:
        record_data = {
            db_column.name: get_nested_key(record, field, not db_column.nullable)
            for field, db_column in field_mapping
            if field is not None
        }
    except KeyError:
        logger.warning(
            f"Failed to load item {record.get('id', str(record))}, required field is missing"
        )
        raise

    return record_data


def _insert_related_records(
    conn: sa.engine.Connection,
    table_config: TableConfig,
    contexts: Tuple[Dict, ...],
):
    for key, related_table in table_config.related_table_configs:
        related_records = get_nested_key(contexts[-1], key) or []

        for related_record in related_records:
            for transform in related_table.transforms:
                related_record = transform(
                    related_record, related_table.field_mapping, contexts
                )

            conn.execute(
                related_table.temp_table.insert(),
                **_get_data_to_insert(related_table.columns, related_record),
            )

            if related_table.related_table_configs:
                _insert_related_records(
                    conn, related_table, contexts + (related_record,)
                )


def insert_data_into_db(
    target_db: str,
    table: Optional[sa.Table] = None,
    field_mapping: Optional[SingleTableFieldMapping] = None,
    table_config: Optional[TableConfig] = None,
    contexts: Tuple = tuple(),
    **kwargs,
):
    """Insert fetched response data into temporary DB tables.

    Goes through the stored response contents and loads individual
    records into the temporary DB table.

    DB columns are populated according to the field mapping, which
    if as list of `(response_field, column)` tuples, where field
    can either be a string or a tuple of keys/indexes forming a
    path for a nested value.

    """
    if table_config:
        if table or field_mapping:
            raise RuntimeError(
                "You must exclusively provide either (table_config) or (table && field_mapping), not bits of both."
            )

        table_config.configure(**kwargs)
        s3 = S3Data(table_config.table_name, kwargs["ts_nodash"])

    elif table is not None and field_mapping is not None:
        warnings.warn(
            (
                "`table` and `field_mapping` parameters are deprecated. "
                "This pipeline should be migrated to use `table_config`/`TableConfig`."
            ),
            DeprecationWarning,
        )

        s3 = S3Data(table.name, kwargs["ts_nodash"])
        temp_table = get_temp_table(table, kwargs["ts_nodash"])

    else:
        raise RuntimeError(
            f"No complete table/field mapping configuration provided: {table}, {field_mapping}"
        )

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    count = 0
    for page, records in s3.iter_keys():
        logger.info(f'Processing page {page}')
        count += 1

        with engine.begin() as conn:
            if table_config:
                for record in records:
                    for transform in table_config.transforms:
                        record = transform(record, table_config, contexts)

                    conn.execute(
                        table_config.temp_table.insert(),
                        **_get_data_to_insert(table_config.columns, record),
                    )

                    if table_config.related_table_configs:
                        _insert_related_records(
                            conn, table_config, contexts + (record,)
                        )

            elif table is not None and field_mapping:
                for record in records:
                    conn.execute(
                        temp_table.insert(),
                        **_get_data_to_insert(field_mapping, record),
                    )

        logger.info(f'Page {page} ingested successfully')

    if count == 0:
        raise MissingDataError("There are no pages of records in S3 to insert.")


def insert_csv_data_into_db(
    target_db: str, table_config: TableConfig, contexts: Tuple = tuple(), **kwargs,
):
    """Insert fetched response data into temporary DB tables.

    Goes through the stored response contents and loads individual
    records into the temporary DB table.

    DB columns are populated according to the field mapping, which
    if as list of `(response_field, column)` tuples, where field
    can either be a string or a tuple of keys/indexes forming a
    path for a nested value.

    """
    table_config.configure(**kwargs)
    s3 = S3Data(table_config.table_name, kwargs["ts_nodash"])

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    count = 0
    done = 0
    for page, data in s3.iter_keys(json=False):
        logger.info(f'Processing page {page}')
        count += 1

        with engine.begin() as conn:
            reader = csv.DictReader(StringIO(data))

            while True:
                records = list(itertools.islice(reader, 10000))
                if not records:
                    break

                logger.info(f"Ingesting records {done} - {done+len(records)} ...")
                transformed_records = []
                for record in records:
                    for transform in table_config.transforms:
                        record = transform(record, table_config, contexts)  # type: ignore
                    transformed_records.append(record)

                conn.execute(
                    table_config.temp_table.insert(),
                    [
                        _get_data_to_insert(table_config.columns, record)
                        for record in transformed_records
                    ],
                )
                done += len(records)

        logger.info(f'File {page} ingested successfully')

    if count == 0:
        raise MissingDataError("There are no pages of records in S3 to insert.")


def _check_table(
    engine, conn, temp: sa.Table, target: sa.Table, allow_null_columns: bool
):
    logger.info(f"Checking {temp.name}")

    if engine.dialect.has_table(conn, target.name, schema=target.schema):
        logger.info("Checking record counts")
        temp_count = conn.execute(
            sa.select([sa.func.count()]).select_from(temp)
        ).fetchone()[0]
        target_count = conn.execute(
            sa.select([sa.func.count()]).select_from(target)
        ).fetchone()[0]

        logger.info(
            "Current records count {}, new import count {}".format(
                target_count, temp_count
            )
        )

        if target_count > 0 and temp_count / target_count < 0.9:
            raise MissingDataError("New record count is less than 90% of current data")

    logger.info("Checking for empty columns")
    for col in temp.columns:
        row = conn.execute(
            sa.select([temp]).select_from(temp).where(col.isnot(None)).limit(1)
        ).fetchone()
        if row is None:
            error = f"Column {col} only contains NULL values"
            if allow_null_columns or config.ALLOW_NULL_DATASET_COLUMNS:
                logger.warning(error)
            else:
                raise UnusedColumnError(error)
    logger.info("All columns are used")


def check_table_data(
    target_db: str, *tables: sa.Table, allow_null_columns: bool = False, **kwargs
):
    """Verify basic constraints on temp table data.

    """

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    with engine.begin() as conn:
        for table in tables:
            temp_table = get_temp_table(table, kwargs["ts_nodash"])
            _check_table(engine, conn, temp_table, table, allow_null_columns)


def query_database(
    query: str, target_db: str, table_name, batch_size: int = 100000, **kwargs
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    total_records = 0
    next_batch = 1
    connection = PostgresHook(postgres_conn_id=target_db).get_conn()

    try:
        # create connection with named cursor to fetch data in batches
        cursor = connection.cursor(name='query_database')
        cursor.execute(query)

        rows = cursor.fetchmany(batch_size)
        fields = [d[0] for d in cursor.description]
        while len(rows):
            records = []
            for row in rows:
                record = {fields[col]: row[col] for col in range(len(row))}
                records.append(record)
            s3.write_key(f'{next_batch:010}.json', records)
            next_batch += 1
            total_records += len(records)
            rows = cursor.fetchmany(batch_size)
    finally:
        if connection:
            cursor.close()
            connection.close()


def swap_dataset_tables(target_db: str, *tables: sa.Table, **kwargs):
    """Rename temporary tables to replace current dataset one.

    Given a one or more dataset tables `tables` this finds the temporary table created
    for the current DAG run and replaces existing dataset one with it.

    If a dataset table didn't exist the new table gets renamed, otherwise
    the existing dataset table is renamed to a temporary "swap" name first.

    This requires an exclusive lock for the dataset table (similar to TRUNCATE)
    but doesn't need to copy any data around (reducing the amount of time dataset
    is unavailable) and will update the table schema at the same time (since it
    will apply the new schema temporary table was created with).

    """
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    for table in tables:
        temp_table = get_temp_table(table, kwargs["ts_nodash"])

        logger.info(f"Moving {temp_table.name} to {table.name}")
        with engine.begin() as conn:
            conn.execute("SET statement_timeout = 600000")
            grantees = [
                grantee[0]
                for grantee in conn.execute(
                    """
                SELECT grantee
                FROM information_schema.role_table_grants
                WHERE table_name='{table_name}'
                AND privilege_type = 'SELECT'
                AND grantor != grantee
                """.format(
                        table_name=engine.dialect.identifier_preparer.quote(table.name)
                    )
                ).fetchall()
            ]

            conn.execute(
                """
                ALTER TABLE IF EXISTS {schema}.{target_temp_table} RENAME TO {swap_table_name};
                ALTER TABLE {schema}.{temp_table} RENAME TO {target_temp_table};
                """.format(
                    schema=engine.dialect.identifier_preparer.quote(table.schema),
                    target_temp_table=engine.dialect.identifier_preparer.quote(
                        table.name
                    ),
                    swap_table_name=engine.dialect.identifier_preparer.quote(
                        temp_table.name + "_swap"
                    ),
                    temp_table=engine.dialect.identifier_preparer.quote(
                        temp_table.name
                    ),
                )
            )
            for grantee in grantees + config.DEFAULT_DATABASE_GRANTEES:
                conn.execute(
                    'GRANT SELECT ON {schema}.{table_name} TO {grantee}'.format(
                        schema=engine.dialect.identifier_preparer.quote(table.schema),
                        table_name=engine.dialect.identifier_preparer.quote(table.name),
                        grantee=grantee,
                    )
                )


def drop_temp_tables(target_db: str, *tables, **kwargs):
    """Delete temporary dataset DB tables.

    Given a dataset table `table`, deletes any related temporary
    tables created during the DAG run.

    """
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    with engine.begin() as conn:
        conn.execute("SET statement_timeout = 600000")
        for table in tables:
            temp_table = get_temp_table(table, kwargs["ts_nodash"])
            logger.info(f"Removing {temp_table.name}")
            temp_table.drop(conn, checkfirst=True)


def drop_swap_tables(target_db: str, *tables, **kwargs):
    """Delete temporary swap dataset DB tables.

    Given a dataset table `table`, deletes any related swap tables
    containing the previous version of the dataset.

    """
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    with engine.begin() as conn:
        conn.execute("SET statement_timeout = 600000")
        for table in tables:
            swap_table = get_temp_table(table, kwargs["ts_nodash"] + "_swap")
            logger.info(f"Removing {swap_table.name}")
            swap_table.drop(conn, checkfirst=True)
