import base64
import itertools
from datetime import timedelta

import backoff
import requests

from dataflow import config
from dataflow.utils import S3Data, S3Upstream, logger


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _query(url, account):
    email = config.ZENDESK_CREDENTIALS[account]["email"]
    secret = config.ZENDESK_CREDENTIALS[account]["secret"]
    token = base64.b64encode(f"{email}/token:{secret}".encode()).decode()
    response = requests.get(
        url=url,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.exception(f"Request failed: {response.text}")
        raise
    return response.json()


def _get_ticket_metrics(id: int, account: str):
    base_url = config.ZENDESK_CREDENTIALS[account]["url"]
    ticket_url = f"{base_url}/tickets/{id}/metrics.json"
    response = _query(url=ticket_url, account=account)
    return {
        "solved_at": response["ticket_metric"]["solved_at"],
        "full_resolution_time_in_minutes": response['ticket_metric'][
            "full_resolution_time_in_minutes"
        ]["calendar"],
    }


def _remove_covid_related_tickets(tickets: list):
    return [
        ticket
        for ticket in tickets
        if ticket["via"]["source"]["to"].get("address")
        != config.ZENDESK_COVID_EMAIL_ADDRESS
    ]


def fetch_daily_tickets(
    schema_name: str, table_name: str, account: str, **kwargs,
):
    """
    Download zendesk json data and reformat it for ingestion into table
    """

    today = kwargs["execution_date"]
    yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"Fetching data from source for day '{yesterday}' on '{today}'")

    results = []
    query = f"type:ticket updated:{yesterday} status:closed"
    base_url = config.ZENDESK_CREDENTIALS[account]["url"]

    for page in itertools.count(1):
        url = f"{base_url}/search.json?query={query}&sort_by=created_at&sort_order=asc&page={page}"
        data = _query(url=url, account=account)
        results.extend(data["results"])
        if not data["next_page"]:
            break
        if page >= 11:
            raise Exception("Too many iterations")

    # Get metrics fields which aren't returned from the search endpoint
    for ticket in results:
        metrics = _get_ticket_metrics(id=ticket["id"], account=account)
        ticket.update(metrics)

    # Remove covid-19 related tickets from dit zendesk tickets
    if account == 'dit':
        results = _remove_covid_related_tickets(results)

    s3upstream = S3Upstream(f"{schema_name}_{table_name}")
    s3upstream.write_key(f"{yesterday}.json", results, jsonify=True)
    logger.info("Fetching from source completed")

    s3data = S3Data(table_name, kwargs["ts_nodash"])
    for source in s3upstream.list_keys():
        s3data.client.copy_object(
            source_bucket_key=source,
            dest_bucket_key=source.replace(s3upstream.prefix, s3data.prefix),
            source_bucket_name=s3data.bucket,
            dest_bucket_name=s3data.bucket,
        )
    logger.info("Copy from upstream completed")
