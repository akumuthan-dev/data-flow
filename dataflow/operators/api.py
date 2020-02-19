import json
import logging

import backoff
import requests
from mohawk import Sender


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _hawk_api_request(
    url: str,
    method: str,
    query: dict,
    credentials: dict,
    expected_response_structure: str = None,
):
    body = json.dumps(query)
    header = Sender(
        credentials, url, method.lower(), content_type="application/json", content=body,
    ).request_header

    response = requests.request(
        method,
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
    if expected_response_structure and expected_response_structure not in response_json:
        raise ValueError("Unexpected response structure")

    return response_json
