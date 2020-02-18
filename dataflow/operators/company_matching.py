import logging
import re

from airflow.hooks.postgres_hook import PostgresHook

from dataflow import config
from dataflow.operators.api import _hawk_api_request
from dataflow.utils import S3Data

credentials = {
    'id': config.MATCHING_SERVICE_HAWK_ID,
    'key': config.MATCHING_SERVICE_HAWK_KEY,
    'algorithm': config.MATCHING_SERVICE_HAWK_ALGORITHM,
}
valid_email = re.compile(r"[^@]+@[^@]+\.[^@]+")


def fetch_from_company_matching(
    target_db: str, table_name: str, company_match_query: str, batch_size=str, **kwargs
):
    logging.info(f"starting company matching")

    s3 = S3Data(table_name, kwargs["ts_nodash"])
    next_batch = 1
    try:

        # create connection with named cursor to fetch data in batches
        connection = PostgresHook(postgres_conn_id=target_db).get_conn()
        cursor = connection.cursor(name='fetch_companies')
        cursor.execute(company_match_query)

        for request in _build_request(
            cursor, batch_size, config.MATCHING_SERVICE_UPDATE
        ):
            match_type = 'update' if config.MATCHING_SERVICE_UPDATE else 'match'
            data = _hawk_api_request(
                url=f'{config.MATCHING_SERVICE_BASE_URL}/api/v1/company/{match_type}/',
                method='POST',
                query=request,
                credentials=credentials,
                expected_response_structure='matches',
            )
            s3.write_key(f"{next_batch:010}.json", data['matches'])
            next_batch += 1
    finally:
        if connection:
            cursor.close()
            connection.close()


def _build_request(cursor, batch_size, update):
    batch_count = 0
    while True:
        descriptions = []
        request = {'descriptions': descriptions}
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        logging.info(
            f"matching companies {f'{batch_count*batch_size}-{batch_count*batch_size+len(rows)}'}"
        )
        for row in rows:
            id = row[0]
            company_name = row[1]
            contact_email = row[2]
            cdms_ref = row[3]
            postcode = row[4]
            companies_house_id = row[5]
            source = row[6]
            datetime = row[7]

            if update and (not datetime or not source):
                continue
            if not id or (
                not company_name
                and not valid_email.match(contact_email or '')
                and not cdms_ref
                and not postcode
                and not len(companies_house_id or '') == 8
            ):
                continue
            description = {
                'id': str(id),
                'source': source,
            }
            if company_name:
                description['company_name'] = company_name
            if contact_email and valid_email.match(contact_email):
                description['contact_email'] = contact_email
            if cdms_ref:
                description['cdms_ref'] = str(cdms_ref)
            if postcode:
                description['postcode'] = postcode
            if companies_house_id and len(companies_house_id) == 8:
                description['companies_house_id'] = companies_house_id
            if datetime:
                description['datetime'] = datetime.strftime("%Y-%m-%d %H:%M:%S")
            descriptions.append(description)
        yield request
        batch_count += 1
