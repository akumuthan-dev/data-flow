import logging
import sqlalchemy as sa
from airflow.hooks.postgres_hook import PostgresHook

from dataflow.utils import get_nested_key, FieldMapping, S3Data
from dataflow.operators.utils import CreateView, create_timestamped_view_name


class MissingDataError(ValueError):
    pass


class UnusedColumnError(ValueError):
    pass


def _get_temp_table(table, suffix, schema='public'):
    """Get a Table object for the temporary dataset table.

    Given a dataset `table` instance creates a new table with
    a unique temporary name for the given DAG run and the same
    columns as the dataset table.

    """
    return sa.Table(
        f"{table.name}_{suffix}".lower(),
        table.metadata,
        schema=schema,
        *[column.copy() for column in table.columns],
    )


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
        for table in tables:
            table = _get_temp_table(table, kwargs["ts_nodash"])
            logging.info(f"Creating {table.name}")
            table.create(conn, checkfirst=True)


def insert_data_into_db(
    target_db: str, table: sa.Table, field_mapping: FieldMapping, **kwargs
):
    """Insert fetched response data into temporary DB tables.

    Goes through the stored response contents and loads individual
    records into the temporary DB table.

    DB columns are populated according to the field mapping, which
    if as list of `(response_field, column)` tuples, where field
    can either be a string or a tuple of keys/indexes forming a
    path for a nested value.

    """
    s3 = S3Data(table.name, kwargs["ts_nodash"])

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    temp_table = _get_temp_table(table, kwargs["ts_nodash"])

    for page, records in s3.iter_keys():
        logging.info(f'Processing page {page}')

        with engine.begin() as conn:
            for record in records:
                try:
                    record_data = {
                        db_column.name: get_nested_key(
                            record, field, not db_column.nullable
                        )
                        for field, db_column in field_mapping
                        if field is not None
                    }
                except KeyError:
                    logging.warning(
                        f"Failed to load item {record.get('id', '')}, required field is missing"
                    )
                    raise
                conn.execute(temp_table.insert(), **record_data)

        logging.info(f'Page {page} ingested successfully')


def _check_table(
    engine, conn, temp: sa.Table, target: sa.Table, check_empty_columns: bool = True
):
    logging.info(f"Checking {temp.name}")

    if engine.dialect.has_table(conn, target.name):
        logging.info("Checking record counts")
        temp_count = conn.execute(
            sa.select([sa.func.count()]).select_from(temp)
        ).fetchone()[0]
        target_count = conn.execute(
            sa.select([sa.func.count()]).select_from(target)
        ).fetchone()[0]

        logging.info(
            "Current records count {}, new import count {}".format(
                target_count, temp_count
            )
        )

        if target_count > 0 and temp_count / target_count < 0.9:
            raise MissingDataError("New record count is less than 90% of current data")

    if check_empty_columns:
        logging.info("Checking for empty columns")
        for col in temp.columns:
            row = conn.execute(
                sa.select([temp]).select_from(temp).where(col.isnot(None)).limit(1)
            ).fetchone()
            if row is None:
                raise UnusedColumnError(f"Column {col} only contains NULL values")
        logging.info("All columns are used")


def check_table_data(
    target_db: str, *tables: sa.Table, check_empty_columns: bool = False, **kwargs
):
    """Verify basic constraints on temp table data.

    """

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )

    with engine.begin() as conn:
        for table in tables:
            temp_table = _get_temp_table(table, kwargs["ts_nodash"])
            _check_table(
                engine, conn, temp_table, table, check_empty_columns=check_empty_columns
            )


def swap_dataset_table(target_db: str, table: sa.Table, **kwargs):
    """Rename temporary table to replace current dataset one.

    Given a dataset table `table` this finds the temporary table created
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
    temp_table = _get_temp_table(table, kwargs["ts_nodash"])

    logging.info(f"Moving {temp_table.name} to {table.name}")
    with engine.begin() as conn:
        conn.execute(
            """
            ALTER TABLE IF EXISTS {target_temp_table} RENAME TO {swap_table_name};
            ALTER TABLE {temp_table} RENAME TO {target_temp_table};
            """.format(
                target_temp_table=engine.dialect.identifier_preparer.quote(table.name),
                swap_table_name=engine.dialect.identifier_preparer.quote(
                    temp_table.name + "_swap"
                ),
                temp_table=engine.dialect.identifier_preparer.quote(temp_table.name),
            )
        )


def drop_temp_tables(target_db: str, *tables, **kwargs):
    """Delete temporary dataset DB tables.

    Given a dataset table `table`, deletes any related temporary
    tables created during the DAG run.

    This includes a temporary table created for the run (if the DAG run
    failed) and the swap table containing the previous version of the dataset
    (if the DAG run succeeded and the dataset table has been replaced).
    """
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    with engine.begin() as conn:
        for table in tables:
            temp_table = _get_temp_table(table, kwargs["ts_nodash"])
            logging.info(f"Removing {temp_table.name}")
            temp_table.drop(conn, checkfirst=True)

            swap_table = _get_temp_table(table, kwargs["ts_nodash"] + "_swap")
            logging.info(f"Removing {swap_table.name}")
            swap_table.drop(conn, checkfirst=True)


def create_static_view_from_table(
    target_db: str, from_table: sa.Table, to_schema: str, view_name: str, **kwargs
):
    """Create a static view based on a dataset pipeline table by...

    1. Copying the dataset table and it's data to a new schema
    2. Creating a new view in the public schema on the newly copied table
    """
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    with engine.begin() as conn:
        to_table = _get_temp_table(from_table, kwargs['ts_nodash'], to_schema)
        to_table.create(conn, checkfirst=True)
        conn.execute(
            to_table.insert().from_select(
                names=from_table.columns, select=from_table.select()
            )
        )
        conn.execute(
            CreateView(
                create_timestamped_view_name(view_name, kwargs['execution_date']),
                to_table.select(),
                materialized=True,
            )
        )
