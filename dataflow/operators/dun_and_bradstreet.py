import sqlalchemy as sa
from airflow.hooks.postgres_hook import PostgresHook

from dataflow.utils import get_temp_table, logger


def update_table(target_db: str, target_table: sa.Table, update_query: str, **kwargs):
    """
    Run a query to update an existing table from a temporary table.
    """
    engine = sa.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
    )
    with engine.begin() as conn:
        from_table = get_temp_table(target_table, kwargs["ts_nodash"])
        logger.info(f'Updating {target_table.name} from {from_table.name}')
        conn.execute(
            update_query.format(
                schema=engine.dialect.identifier_preparer.quote(target_table.schema),
                target_table=engine.dialect.identifier_preparer.quote(
                    target_table.name
                ),
                from_table=engine.dialect.identifier_preparer.quote(from_table.name),
            )
        )
