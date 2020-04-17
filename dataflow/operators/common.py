from typing import Optional

import backoff
import requests
from mohawk import Sender
from mohawk.exc import HawkFail

from dataflow.utils import S3Data, get_nested_key, logger


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _hawk_api_request(
    url: str,
    credentials: dict,
    results_key: Optional[str],
    next_key: Optional[str],
    validate_response: Optional[bool] = True,
):
    sender = Sender(
        credentials, url, "get", content="", content_type="", always_hash_content=True
    )

    logger.info(f"Fetching page {url}")
    response = requests.get(
        url, headers={"Authorization": sender.request_header, "Content-Type": ""}
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error(f"Request failed: {response.text}")
        raise

    if validate_response:
        try:
            sender.accept_response(
                response.headers["Server-Authorization"],
                content=response.content,
                content_type=response.headers["Content-Type"],
            )
        except HawkFail as e:
            logger.error(f"HAWK Authentication failed {str(e)}")
            raise

    response_json = response.json()

    if (next_key and next_key not in response_json) or (
        results_key and results_key not in response_json
    ):
        raise ValueError("Unexpected response structure")

    return response_json


def fetch_from_hawk_api(
    table_name: str,
    source_url: str,
    hawk_credentials: dict,
    results_key: str = "results",
    next_key: Optional[str] = "next",
    validate_response: Optional[bool] = True,
    **kwargs,
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    total_records = 0
    page = 1

    while True:
        data = _hawk_api_request(
            source_url,
            credentials=hawk_credentials,
            results_key=results_key,
            next_key=next_key,
            validate_response=validate_response,
        )

        results = get_nested_key(data, results_key)
        s3.write_key(f"{page:010}.json", results)

        total_records += len(results)
        logger.info(f"Fetched {total_records} records")

        source_url = get_nested_key(data, next_key) if next_key else None
        if not source_url:
            break

        page += 1

    logger.info("Fetching from source completed")
