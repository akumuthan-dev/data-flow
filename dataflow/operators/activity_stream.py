import logging

from dataflow import config
from dataflow.operators.api import _hawk_api_request
from dataflow.utils import S3Data


def fetch_from_activity_stream(table_name: str, index_name: str, query: dict, **kwargs):

    s3 = S3Data(table_name, kwargs["ts_nodash"])

    query = {
        "query": query,
        "size": config.ACTIVITY_STREAM_RESULTS_PER_PAGE,
        "sort": [{"id": "asc"}],
    }
    next_page = 1

    source_url = f"{config.ACTIVITY_STREAM_BASE_URL}/v3/{index_name}/_search"

    while next_page:
        logging.info(f"Fetching page {next_page} of {source_url}")
        data = _hawk_api_request(
            source_url,
            "GET",
            query,
            {
                "id": config.ACTIVITY_STREAM_ID,
                "key": config.ACTIVITY_STREAM_SECRET,
                "algorithm": config.HAWK_ALGORITHM,
            },
            'hits',
        )
        if "failures" in data["_shards"]:
            logging.warning(
                "Request failed on {} shards: {}".format(
                    data['_shards']['failed'], data['_shards']['failures']
                )
            )

        if not data["hits"]["hits"]:
            next_page = 0
            continue

        s3.write_key(
            f"{next_page:010}.json", [item["_source"] for item in data["hits"]["hits"]]
        )

        logging.info(
            f"Fetched {len(data['hits']['hits'])} of {data['hits']['total']} records"
        )

        query = query.copy()
        query["search_after"] = data["hits"]["hits"][-1]["sort"]

        next_page += 1

    logging.info(f"Fetching from source completed, total {data['hits']['total']}")
