import glob
import os
import shutil
import subprocess
from tempfile import TemporaryDirectory
from typing import Optional

import backoff
import requests

from dataflow import config
from dataflow.utils import logger, S3Data


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _ons_sparql_request(url: str, query: str, page: int = 1, per_page: int = 10000):
    query += f" LIMIT {per_page} OFFSET {per_page * (page - 1)}"
    response = requests.request(
        "POST", url, data={"query": query}, headers={"Accept": "application/json"}
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error(f"Request failed: {response.text}")
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
        logger.info(f"Fetching page {prefix}{next_page}")
        data = _ons_sparql_request(config.ONS_SPARQL_URL, query, page=next_page)

        if not data["results"]["bindings"]:
            next_page = 0
            continue

        total_records += len(data["results"]["bindings"])
        s3.write_key(f"{prefix}{next_page:010}.json", data["results"]["bindings"])

        logger.info(f"Fetched {total_records} records")

        next_page += 1

    logger.info(f"Fetching from source completed, total {total_records}")


def run_ipython_ons_extraction(table_name: str, script_name: str, **kwargs):
    with TemporaryDirectory() as tempdir:
        os.chdir(tempdir)

        shutil.copytree('/app/dataflow/ons_scripts', 'ons_scripts')

        logger.info("ONS scraper: start")
        subprocess.call(['ipython', 'main.py'], cwd=f'ons_scripts/{script_name}')
        logger.info("ONS scraper: completed")

        s3 = S3Data(table_name, kwargs['ts_nodash'])

        for filename in sorted(
            glob.glob(f"ons_scripts/{script_name}/out/observations*.csv")
        ):
            logger.info(f"Writing {filename} to S3.")
            s3.write_key(
                os.path.basename(filename), open(filename, "r").read(), jsonify=False,
            )
