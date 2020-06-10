import codecs
import csv
from contextlib import closing
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
    force_http: Optional[bool] = False,
):
    sender = Sender(
        credentials,
        # Currently data workspace denies hawk requests signed with https urls.
        # Once fixed the protocol replacement can be removed.
        url.replace('https', 'http') if force_http else url,
        "get",
        content="",
        content_type="",
        always_hash_content=True,
    )

    logger.info(f"Fetching page {url}")
    response = requests.get(
        url,
        headers={"Authorization": sender.request_header, "Content-Type": ""},
        timeout=300,
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
    force_http: Optional[bool] = False,
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
            force_http=force_http,
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


def fetch_from_api_endpoint(
    table_name: str,
    source_url: str,
    auth_token: Optional[str] = None,
    results_key: Optional[str] = "results",
    next_key: Optional[str] = "next",
    **kwargs,
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    total_records = 0
    page = 1

    while True:
        response = requests.get(
            source_url,
            headers={'Authorization': f'Token {auth_token}'} if auth_token else None,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error(f"Request failed: {response.text}")
            raise

        response_json = response.json()

        if (next_key and next_key not in response_json) or (
            results_key and results_key not in response_json
        ):
            raise ValueError("Unexpected response structure")

        if results_key is not None:
            results = get_nested_key(response_json, results_key)
        else:
            results = response_json

        s3.write_key(f"{page:010}.json", results)

        total_records += len(results)
        logger.info(f"Fetched {total_records} records")

        source_url = get_nested_key(response_json, next_key) if next_key else None
        if not source_url:
            break

        page += 1

    logger.info("Fetching from source completed")


def fetch_from_hosted_csv(
    table_name: str,
    source_url: str,
    page_size: int = 1000,
    allow_empty_strings: bool = True,
    **kwargs,
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    results = []
    page = 1
    with closing(requests.get(source_url, stream=True)) as request:
        reader = csv.DictReader(codecs.iterdecode(request.iter_lines(), 'utf-8'))
        for row in reader:
            if not allow_empty_strings:
                row = {k: v if v != '' else None for k, v in row.items()}  # type: ignore
            results.append(row)
            if len(results) >= page_size:
                s3.write_key(f"{page:010}.json", results)
                results = []
                page += 1
        if results:
            s3.write_key(f"{page:010}.json", results)

    logger.info("Fetching from source completed")
