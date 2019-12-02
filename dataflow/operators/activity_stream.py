import json
import logging

import backoff
import requests
from airflow.models import Variable
from mohawk import Sender

from dataflow import config
from .dataset import get_redis_client


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _activity_stream_request(url: str, query: dict):
    body = json.dumps(query)
    header = Sender(
        {
            "id": config.ACTIVITY_STREAM_ID,
            "key": config.ACTIVITY_STREAM_SECRET,
            "algorithm": config.HAWK_ALGORITHM,
        },
        url,
        "get",
        content_type="application/json",
        content=body,
    ).request_header

    response = requests.request(
        "GET",
        url,
        data=body,
        headers={"Authorization": header, "Content-Type": "application/json"},
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error(f"Request failed: {response.text}")
        raise

    response_json = response.json()
    if "hits" not in response_json:
        raise ValueError("Unexpected response structure")

    return response_json


def fetch_from_activity_stream(
    index_name: str, query: dict, run_fetch_task_id: str, **kwargs
):
    redis_client = get_redis_client()
    # Clear any leftover requests from previous task runs
    redis_client.delete(run_fetch_task_id)

    query = {
        "query": query,
        "size": config.ACTIVITY_STREAM_RESULTS_PER_PAGE,
        "sort": [{"id": "asc"}],
    }
    next_page = 1

    source_url = f"{config.ACTIVITY_STREAM_BASE_URL}/v3/{index_name}/_search"

    while next_page:
        logging.info(f"Fetching page {next_page} of {source_url}")
        data = _activity_stream_request(source_url, query)
        if "failures" in data["_shards"]:
            logging.warning(
                "Request failed on {} shards: {}".format(
                    data['_shards']['failed'], data['_shards']['failures']
                )
            )
        key = f"{run_fetch_task_id}_{next_page}"

        if not data["hits"]["hits"]:
            next_page = 0
            continue

        logging.info(
            f"Fetched {len(data['hits']['hits'])} of {data['hits']['total']} records"
        )

        Variable.set(
            key, [item["_source"] for item in data["hits"]["hits"]], serialize_json=True
        )
        redis_client.rpush(run_fetch_task_id, key)

        query = query.copy()
        query["search_after"] = data["hits"]["hits"][-1]["sort"]

        next_page += 1

    logging.info(f"Fetching from source completed, total {data['hits']['total']}")
