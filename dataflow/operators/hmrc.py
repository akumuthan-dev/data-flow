import io
import logging
import zipfile
from datetime import datetime
from typing import BinaryIO, cast

import backoff
import pytz
import requests
from dateutil.relativedelta import relativedelta
from dateutil.rrule import MONTHLY, rrule

from dataflow import config
from dataflow.utils import logger, S3Data


def fetch_hmrc_trade_data(
    table_name: str, base_filename: str, records_start_date: datetime, **kwargs
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])

    # New files are uploaded to uktradeinfo 2 months later, usually on 10th of the month.
    latest_file_date = kwargs.get(
        "run_date", kwargs.get("execution_date")
    ) - relativedelta(months=1)

    for page, run_date in enumerate(
        rrule(
            MONTHLY,
            dtstart=records_start_date.replace(tzinfo=pytz.UTC),
            until=latest_file_date,
        )
    ):
        source_url = f"{config.HMRC_UKTRADEINFO_URL}/{base_filename}{run_date:%y%m}.zip"
        logger.info("Fetching %s (page: %s)", source_url, page)
        file_contents = _download(source_url)
        with zipfile.ZipFile(io.BytesIO(file_contents)) as archive:
            with archive.open(archive.namelist()[0], "r") as f:
                data = [
                    r.strip().split("|")
                    for r in io.TextIOWrapper(
                        cast(BinaryIO, f), encoding='utf-8'
                    ).readlines()
                ]
        output_filename = f"{page:010}.json"
        s3.write_key(output_filename, data[1:-1])
        logging.info("Fetched from source to %s", output_filename)


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download(source_url):
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error("Request failed: %s", response.text)
        raise

    return response.content
