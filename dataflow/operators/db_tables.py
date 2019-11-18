import logging

import sqlalchemy as sa
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from dataflow.utils import get_nested_key, get_redis_client, FieldMapping


def _get_temp_table(table, suffix):
    """Get a Table object for the temporary dataset table.

    Given a dataset `table` instance creates a new table with
    a unique temporary name for the given DAG run and the same
    columns as the dataset table.

    """
    return sa.Table(
        f"{table.name}_{suffix}".lower(),
        table.metadata,
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
    target_db: str,
    table: sa.Table,
    field_mapping: FieldMapping,
    run_fetch_task_id: str,
    **kwargs,
):
    """Insert fetched response data into temporary DB tables.

    Goes through the stored reponse contents and loads individual
    records into the temporary DB table.

    DB columns are populated according to the field mapping, which
    if as list of `(response_field, column)` tuples, where field
    can either be a string or a tuple of keys/indexes forming a
    path for a nested value.

    """
    redis_client = get_redis_client()

    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    table = _get_temp_table(table, kwargs["ts_nodash"])

    var_names = redis_client.lrange(run_fetch_task_id, 0, -1)

    for var_name in var_names:
        logging.info(f'Processing page {var_name}')

        record_subset = Variable.get(var_name, deserialize_json=True)
        with engine.begin() as conn:
            for record in record_subset:
                try:
                    record_data = {
                        db_column.name: get_nested_key(
                            record, field, not db_column.nullable
                        )
                        for field, db_column in field_mapping
                    }
                except KeyError:
                    logging.warning(
                        f"Failed to load item {record.get('id', '')}, required field is missing"
                    )
                    raise
                conn.execute(table.insert(), **record_data)

        logging.info(f'Page {var_name} ingested successfully')
        Variable.delete(var_name)

    redis_client.delete(run_fetch_task_id)


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
