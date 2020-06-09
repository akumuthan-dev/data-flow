import io
import logging
import zipfile
from datetime import timedelta
from typing import cast

import backoff
import requests
from typing.io import IO

from dataflow import config
from dataflow.utils import logger, S3Data


def fetch_uktradeinfo_non_eu_data(table_name: str, base_filename: str, **kwargs):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    # New files are uploaded to uktradeinfo 2 months later, usually on 10th of the month.
    # 45 day offest allows us to pick up new files within 5 days of them being uploaded
    # without parsing the downloads page.
    run_date = kwargs.get("run_date", kwargs.get("execution_date")) - timedelta(days=60)
    filename = f"{base_filename}{run_date:%y%m}"
    source_url = f"{config.HMRC_UKTRADEINFO_URL}/{filename}.zip"

    logger.info(f"Fetching {source_url}")

    file_contents = _download(source_url)

    with zipfile.ZipFile(io.BytesIO(file_contents)) as archive:
        with archive.open(archive.namelist()[0], "r") as f:
            data = [
                r.strip().split("|")
                for r in io.TextIOWrapper(
                    cast(IO[bytes], f), encoding='utf-8'
                ).readlines()
            ]

    s3.write_key(f"{filename}.json", data[1:-1])

    logging.info(f"Fetched from source to {filename}")


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download(source_url):
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error(f"Request failed: {response.text}")
        raise

    return response.content
