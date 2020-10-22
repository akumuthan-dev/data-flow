from airflow.hooks.postgres_hook import PostgresHook
import sqlalchemy

from dataflow import config
from dataflow.dags.consent_pipelines import ConsentPipeline
from dataflow.utils import get_temp_table, logger


def update_datahub_contact_consent(
    target_db: str,
    table: sqlalchemy.Table,
    **kwargs,
):
    """
    Updates Contacts temp table with email marketing consent data from Consent dataset.
    """
    table = get_temp_table(table, kwargs['ts_nodash'])
    update_consent_query = f"""
        UPDATE {table.name} AS contacts_temp
        SET email_marketing_consent = consent.email_marketing_consent
        FROM {ConsentPipeline.fq_table_name()} AS consent
        WHERE lower(contacts_temp.email) = lower(consent.email)
    """
    engine = sqlalchemy.create_engine(
        'postgresql+psycopg2://',
        creator=PostgresHook(postgres_conn_id=target_db).get_conn,
        echo=config.DEBUG,
    )
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(update_consent_query))

    logger.info('Updated Contacts temp table with email consent from Consent dataset')
