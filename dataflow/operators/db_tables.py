import datetime
import os
import warnings
from collections import defaultdict
from hashlib import md5
from time import sleep
from typing import Tuple, Dict, Optional, TYPE_CHECKING, List
from urllib.parse import urlparse

import psycopg3
import sqlalchemy as sa
from sqlalchemy import text, Index
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.exceptions import TaskNotFound


from dataflow import config

from dataflow.utils import (
    get_nested_key,
    get_temp_table,
    S3Data,
    TableConfig,
    SingleTableFieldMapping,
    logger,
    LateIndex,
)

if TYPE_CHECKING:
    from dataflow.dags import _PandasPipelineWithPollingSupport  # noqa


class MissingDataError(ValueError):
    pass


class UnusedColumnError(ValueError):
    pass


def branch_on_modified_date(target_db: str, table_config: TableConfig, **context):
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    with engine.begin() as conn:
        res = conn.execute(
            """
            SELECT source_data_modified_utc
            FROM dataflow.metadata
            WHERE table_schema = %s and table_name = %s
            """,
            [table_config.schema, table_config.table_name],
        ).fetchall()

        if len(res) == 0:
            return 'continue'
        if len(res) > 1:
            raise AirflowException(
                f"Multiple rows in the dataflow metadata table for {table_config.schema}.{table_config.table_name}"
            )
        if not res[0][0]:
            return 'continue'

        old_modified_utc = res[0][0]

    new_modified_utc = context['task_instance'].xcom_pull(
        task_ids='get-source-modified-date'
    )
    context['task_instance'].xcom_push('source-modified-date-utc', new_modified_utc)

    logger.info("Old: %s. New: %s", old_modified_utc, new_modified_utc)

    if new_modified_utc > old_modified_utc:
        return 'continue'

    return 'stop'


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
            logger.info("Creating schema %s if not exists", table.schema)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {table.schema}")
            logger.info("Creating %s", table.name)
            table.create(conn, checkfirst=True)


def create_temp_table_indexes(target_db: str, table_config: TableConfig, **kwargs):
    """
    Applies TableConfig.indexes to a pre-existing and pre-populated table. This is far more efficient for large datasets
    than having the index in-place when data is being inserted.

    This task processes the contents of `TableConfig.indexes`.

    It only applies indexes from the root table in a TableConfig right now.
    """

    if table_config.indexes is None:
        return

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    def _get_sa_index_and_metadata(
        table_config: TableConfig, index: LateIndex
    ) -> Tuple[sa.Index, str, List[str]]:
        """
        Returns a sqlalchemy.Index object for the given LateIndex object. The name is created in a pseudo-random
        way but is reproducible based on the inputs:

        1) Pipeline timestamp
        2) Table name
        3) All of the columns in the index

        The index name created is relatively user-unfriendly, but it avoids the case where the inputs exceed the max
        length of Postgres identifiers and get truncated, which has caused problems historically.
        """
        cols = [index.columns] if isinstance(index.columns, str) else index.columns
        index_name_parts = [table_config.schema, table_config.table_name] + cols
        index_hash = md5('\n'.join(index_name_parts).encode('utf-8')).hexdigest()[:32]
        index_suffix = 'key' if index.unique else 'idx'
        index_type = 'unique index' if index.unique else 'index'
        index_name = f'{kwargs["ts_nodash"]}_{index_hash}_{index_suffix}'
        sa_index = Index(index_name, *cols, unique=index.unique)
        return sa_index, index_type, cols

    with engine.begin() as conn:
        conn.execute(
            "SET statement_timeout = 1800000"
        )  # 30-minute timeout on index creation - should be plenty generous

        table = get_temp_table(table_config.table, kwargs["ts_nodash"])
        indexes = []
        for index in table_config.indexes:
            sa_index, index_type, cols = _get_sa_index_and_metadata(table_config, index)
            table.append_constraint(sa_index)
            indexes.append((sa_index, index_type, cols))

        for sa_index, index_type, cols in indexes:
            logger.info(
                "Creating %s %s on %s (%s)",
                index_type,
                sa_index.name,
                table.fullname,
                ', '.join(cols),
            )
            sa_index.create(conn)


def _get_data_to_insert(field_mapping: SingleTableFieldMapping, record: Dict):
    try:
        record_data = {
            db_column.name: get_nested_key(record, field, not db_column.nullable)
            for field, db_column in field_mapping
            if field is not None
        }
    except KeyError:
        logger.warning(
            "Failed to load item %s, required field is missing",
            record.get('id', str(record)),
        )
        raise

    return record_data


def _insert_related_records(
    conn: sa.engine.Connection, table_config: TableConfig, contexts: Tuple[Dict, ...],
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
        logger.info('Single insert: Processing page %s', page)
        count += 1
        start = datetime.datetime.now()

        with engine.begin() as conn:
            record_count = 0
            if table_config:
                for record in records:
                    record_count += 1
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
                record_count += 1
                for record in records:
                    conn.execute(
                        temp_table.insert(),  # pylint: disable=E1120
                        **_get_data_to_insert(field_mapping, record),
                    )

        logger.info(
            'Single record insert: %s records inserted in %s seconds',
            record_count,
            (datetime.datetime.now() - start).total_seconds(),
        )

    if count == 0:
        raise MissingDataError("There are no pages of records in S3 to insert.")


def _check_table(
    engine, conn, temp: sa.Table, target: sa.Table, allow_null_columns: bool
):
    logger.info("Checking %s", temp.name)

    if engine.dialect.has_table(conn, target.name, schema=target.schema):
        logger.info("Checking record counts")
        temp_count = conn.execute(
            sa.select([sa.func.count()]).select_from(temp)
        ).fetchone()[0]
        target_count = conn.execute(
            sa.select([sa.func.count()]).select_from(target)
        ).fetchone()[0]

        logger.info(
            "Current records count %s, new import count %s", target_count, temp_count
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
    """Verify basic constraints on temp table data."""

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
        while rows:
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


def swap_dataset_tables(
    target_db: str,
    *tables: sa.Table,
    use_utc_now_as_source_modified: bool = False,
    **kwargs,
):
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

        logger.info("Moving %s to %s", temp_table.name, table.name)
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
                SELECT dataflow.save_and_drop_dependencies('{schema}', '{target_temp_table}');
                ALTER TABLE IF EXISTS {schema}.{target_temp_table} RENAME TO {swap_table_name};
                ALTER TABLE {schema}.{temp_table} RENAME TO {target_temp_table};
                SELECT dataflow.restore_dependencies('{schema}', '{target_temp_table}');
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

            new_modified_utc = kwargs['task_instance'].xcom_pull(
                key='source-modified-date-utc'
            )
            if new_modified_utc is None and use_utc_now_as_source_modified:
                try:
                    new_modified_utc = get_task_instance(
                        kwargs['dag'].safe_dag_id,
                        'run-fetch',
                        kwargs['execution_date'],
                    ).end_date
                except TaskNotFound:
                    new_modified_utc = datetime.datetime.utcnow()

            conn.execute(
                """
                INSERT INTO dataflow.metadata
                (table_schema, table_name, source_data_modified_utc, dataflow_swapped_tables_utc)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    table.schema,
                    table.name,
                    new_modified_utc,
                    datetime.datetime.utcnow(),
                ),
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
            logger.info("Removing %s", temp_table.name)
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
            logger.info("Removing %s", swap_table.name)
            swap_table.drop(conn, checkfirst=True)


def poll_for_new_data(
    target_db: str,
    table_config: TableConfig,
    pipeline_instance: "_PandasPipelineWithPollingSupport",
    **kwargs,
):
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    with engine.begin() as conn:
        db_data_last_modified_utc = conn.execute(
            text(
                "SELECT MAX(source_data_modified_utc) "
                "FROM dataflow.metadata "
                "WHERE table_schema = :schema AND table_name = :table"
            ),
            schema=engine.dialect.identifier_preparer.quote(table_config.schema),
            table=engine.dialect.identifier_preparer.quote(table_config.table_name),
        ).fetchall()[0][0]
        logger.info("last_ingest_utc from previous run: %s", db_data_last_modified_utc)

    (
        source_last_modified_utc,
        source_next_release_utc,
    ) = pipeline_instance.__class__.date_checker()
    logger.info(
        "source_last_modified_utc=%s, source_next_release_utc=%s",
        source_last_modified_utc,
        source_next_release_utc,
    )

    if (
        source_next_release_utc
        and source_last_modified_utc == db_data_last_modified_utc
        and datetime.datetime.utcnow().date() < source_next_release_utc.date()
    ):
        logger.info(
            "No new data is available and it is before the next release date, so there's no need to poll. "
            "source_last_modified_utc=%s, source_next_release_utc=%s, db_data_last_modified_utc=%s",
            source_last_modified_utc,
            source_next_release_utc,
            db_data_last_modified_utc,
        )
        pipeline_instance.skip_downstream_tasks(**kwargs)
        return

    while (
        db_data_last_modified_utc
        and source_last_modified_utc <= db_data_last_modified_utc
    ):
        if (
            datetime.datetime.now().time()
            >= pipeline_instance.__class__.daily_end_time_utc
        ):
            logger.info(
                "No newer data has been made available for today, "
                "and it is now after the scheduled end time for polling (%s). "
                "Ending the job and skipping remaining tasks.",
                pipeline_instance.__class__.daily_end_time_utc,
            )
            pipeline_instance.skip_downstream_tasks(**kwargs)
            return

        sleep(pipeline_instance.__class__.polling_interval_in_seconds)
        (
            source_last_modified_utc,
            source_next_release_utc,
        ) = pipeline_instance.__class__.date_checker()
        logger.info(
            "source_last_modified_utc from latest data: %s", source_last_modified_utc
        )

    logger.info("Newer data is available.")
    kwargs['task_instance'].xcom_push(
        'source-modified-date-utc', source_last_modified_utc
    )


def scrape_load_and_check_data(
    target_db: str,
    table_config: TableConfig,
    pipeline_instance: "_PandasPipelineWithPollingSupport",
    **kwargs,
):
    create_temp_tables(target_db, *table_config.tables, **kwargs)

    temp_table = get_temp_table(table_config.table, suffix=kwargs['ts_nodash'])

    data_frames = pipeline_instance.__class__.data_getter()

    parsed_uri = urlparse(os.environ['AIRFLOW_CONN_DATASETS_DB'])
    host, port, dbname, user, password = (
        parsed_uri.hostname,
        parsed_uri.port or 5432,
        parsed_uri.path.strip('/'),
        parsed_uri.username,
        parsed_uri.password,
    )
    # Psycopg3 is still under active development, but crucially has support for generating data and pushing it to
    # postgres efficiently via `cursor.copy` and the COPY protocol.
    with psycopg3.connect(
        f'host={host} port={port} dbname={dbname} user={user} password={password}'
    ) as connection:
        with connection.cursor() as cursor:
            logger.info("Starting streaming copy to DB")

            records_num = 0
            df_num = 0
            with cursor.copy(
                f'COPY "{temp_table.schema}"."{temp_table.name}" FROM STDIN'
            ) as copy:
                for data_frame in data_frames:
                    df_num += 1
                    df_len = len(data_frame)
                    records_num += df_len

                    logger.info(
                        "Copying data frame #%s (records %s - %s)",
                        df_num,
                        records_num - df_len,
                        records_num,
                    )
                    copy.write(
                        data_frame.to_csv(
                            index=False,
                            header=False,
                            sep='\t',
                            na_rep=r'\N',
                            columns=[
                                data_column
                                for data_column, sa_column in table_config.columns
                            ],
                        )
                    )
                    del data_frame

            logger.info("Copy complete.")


def _parse_nested_record(
    table_config: TableConfig, record: dict, contexts: Tuple, table_data: dict = None
):
    """
    For a single record, traverse all tables in a table config and prepare associated data for insert.
    """
    if table_data is None:
        table_data = defaultdict(list)

    for field_map in table_config.field_mapping:
        orig_field, new_field = field_map
        if isinstance(new_field, TableConfig):
            sub_records = get_nested_key(record, orig_field) or []
            for sub_record in sub_records:
                table_data = _parse_nested_record(
                    new_field, sub_record, (record,) + contexts, table_data
                )

    for transform in table_config.transforms:
        record = transform(record, table_config, contexts)

    table_data[table_config.temp_table].append(
        _get_data_to_insert(table_config.columns, record)
    )
    return table_data


def bulk_insert_data_into_db(
    target_db: str, table_config: TableConfig, contexts: Tuple = tuple(), **kwargs,
):
    """
    Insert fetched response data into temporary DB tables in bulk.

    If the TableConfig contains nested tables we parse each record into a
    <SA Table> -> [<data to insert>] dict.

    Once the whole page has been parsed loop through the resulting dict and bulk
    insert the records we have to each table.

    If no related tables exist skip the table traversing stage to speed things up a bit.
    """
    table_config.configure(**kwargs)
    s3 = S3Data(table_config.table_name, kwargs["ts_nodash"])
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    has_sub_tables = bool(table_config.related_table_configs)
    total_records_processed = 0

    for page, records in s3.iter_keys():
        logger.info('Bulk insert: Processing page %s', page)
        start = datetime.datetime.now()
        table_insert_map = defaultdict(list)
        for record in records:
            # If this pipeline contains sub tables build a dict -> data mapping for
            # each table for this page of results
            if has_sub_tables:
                for table, data in _parse_nested_record(
                    table_config, record, contexts + (record,)
                ).items():
                    table_insert_map[table].extend(data)
            # If it's a single table no need to traverse the field mappings so just
            # clean the data ready for insert
            else:
                for transform in table_config.transforms:
                    record = transform(record, table_config, contexts)
                table_insert_map[table_config.temp_table].append(
                    _get_data_to_insert(table_config.columns, record)
                )
        insert_count = 0
        for table, data in table_insert_map.items():
            engine.execute(table.insert(), data)
            insert_count += len(data)

        total_records_processed += insert_count

        logger.info(
            'Bulk insert: %s records inserted into %s tables in %s seconds',
            insert_count,
            len(table_insert_map),
            (datetime.datetime.now() - start).total_seconds(),
        )

    if total_records_processed == 0:
        raise MissingDataError("There are no pages of records in S3 to insert.")

    logger.info('Bulk insert: Finished inserting %s records', total_records_processed)
