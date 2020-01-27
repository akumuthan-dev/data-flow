import logging
from typing import Optional

import backoff
import requests

from dataflow import config
from dataflow.utils import S3Data


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _ons_sparql_request(url: str, query: str, page: int = 1, per_page: int = 10000):
    query += f" LIMIT {per_page} OFFSET {per_page * (page - 1)}"
    response = requests.request(
        "POST", url, data={"query": query}, headers={"Accept": "application/json"}
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error(f"Request failed: {response.text}")
        raise

    response_json = response.json()
    if "results" not in response_json:
        raise ValueError("Unexpected response structure")

    return response_json


def fetch_from_ons_sparql(
    table_name: str, query: str, index_query: Optional[str], **kwargs
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])

    if index_query is None:
        _store_ons_sparql_pages(s3, query)
    else:
        index_values = _ons_sparql_request(config.ONS_SPARQL_URL, index_query)[
            "results"
        ]["bindings"]

        for index in index_values:
            _store_ons_sparql_pages(
                s3, query.format(**index), index['label']['value'] + "-"
            )


def _store_ons_sparql_pages(s3: S3Data, query: str, prefix: str = ""):
    next_page = 1
    total_records = 0

    while next_page:
        logging.info(f"Fetching page {prefix}{next_page}")
        data = _ons_sparql_request(config.ONS_SPARQL_URL, query, page=next_page)

        if not data["results"]["bindings"]:
            next_page = 0
            continue

        total_records += len(data["results"]["bindings"])
        s3.write_key(f"{prefix}{next_page:010}.json", data["results"]["bindings"])

        logging.info(f"Fetched {total_records} records")

        next_page += 1

    logging.info(f"Fetching from source completed, total {total_records}")
