import backoff
from mohawk import Sender
from mohawk.exc import HawkFail
import requests

from dataflow import config
from dataflow.utils import logger, S3Data


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _hawk_api_request(url: str):
    sender = Sender(
        {
            'id': config.HAWK_ID,
            'key': config.HAWK_KEY,
            'algorithm': config.HAWK_ALGORITHM,
        },
        url,
        'get',
        content='',
        content_type='',
        always_hash_content=True,
    )

    logger.info(f'Fetching page {url}')
    response = requests.get(url, headers={'Authorization': sender.request_header})

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error(f"Request failed: {response.text}")
        raise

    try:
        sender.accept_response(
            response.headers['Server-Authorization'],
            content=response.content,
            content_type=response.headers['Content-Type'],
        )
    except HawkFail as e:
        logger.error(f'HAWK Authentication failed {str(e)}')
        raise

    response_json = response.json()

    if 'results' not in response_json or 'next' not in response_json:
        raise ValueError('Unexpected response structure')

    return response_json


def fetch_from_api(table_name: str, source_url: str, **kwargs):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    total_records = 0
    page = 1

    while True:
        data = _hawk_api_request(source_url)
        total_records += len(data["results"])
        s3.write_key(f"{page:010}.json", data["results"])
        logger.info(f"Fetched {total_records} records")
        source_url = data.get('next')
        if not source_url:
            break
        page += 1

    logger.info('Fetching from source completed')
