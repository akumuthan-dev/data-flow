import json
import logging

import requests
from airflow.models import Variable
from mohawk import Sender

from dataflow import config
from .dataset import get_redis_client


def _activity_stream_request(url: str, query: dict):
    body = json.dumps(query)
    header = Sender(
        {
            'id': config.ACTIVITY_STREAM_ID,
            'key': config.ACTIVITY_STREAM_SECRET,
            'algorithm': config.HAWK_ALGORITHM,
        },
        url,
        "get",
        content_type='application/json',
        content=body,
    ).request_header

    response = requests.request(
        "GET",
        url,
        data=body,
        headers={'Authorization': header, 'Content-Type': 'application/json'},
    )
    response.raise_for_status()

    response_json = response.json()
    if 'hits' not in response_json:
        raise ValueError('Unexpected response structure')

    return response_json


def fetch_from_activity_stream(
    index_name: str, query: dict, run_fetch_task_id: str, **kwargs
):
    redis_client = get_redis_client()
    # Clear any leftover requests from previous task runs
    redis_client.delete(run_fetch_task_id)

    query = {**query, "size": config.ACTIVITY_STREAM_RESULTS_PER_PAGE, "from": 0}
    next_page = True

    source_url = f"{config.ACTIVITY_STREAM_BASE_URL}{index_name}"

    while next_page:
        logging.info(f'Fetching page {source_url}, offset {query["from"]}')
        data = _activity_stream_request(source_url, query)

        key = f'{run_fetch_task_id}{query["from"]}'
        Variable.set(
            key, [item['_source'] for item in data['hits']['hits']], serialize_json=True
        )
        redis_client.rpush(run_fetch_task_id, key)

        if data['hits']['hits']:
            query = query.copy()
            query["from"] += query["size"]
        else:
            next_page = False

    logging.info(f'Fetching from source completed, total {data["hits"]["total"]}')
