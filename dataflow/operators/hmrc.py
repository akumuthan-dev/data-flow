import io
import logging
import zipfile
from urllib.parse import urljoin

import backoff
from bs4 import BeautifulSoup
import requests
from dateutil.relativedelta import relativedelta

from dataflow import config
from dataflow.utils import logger, S3Data


def fetch_hmrc_trade_data(
    table_name: str,
    base_filename: str,
    records_start_year: int,
    num_expected_fields: int,
    num_per_page: int = 10000,
    **kwargs,
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])

    # New files are uploaded to uktradeinfo 2 months later, usually on 10th of the month.
    # The files have a predictable name, but not a predictable directory. The best way we have
    # of finding the URLs is to scrape the pages from which they're linked
    #
    # As much as is possible, we stream-process the zips, since some of the unzipped files are
    # > 100mb. Without the streaming, we sometimes hit memory limits when parsing the contents
    latest_file_date = kwargs.get(
        "run_date", kwargs.get("execution_date")
    ) - relativedelta(months=1)
    previous_years = list(range(records_start_year, latest_file_date.year))

    def get_file_linked_from(url, filename):
        logger.info('Looking on %s for links to %s', url, filename)
        html = _download(url)
        soup = BeautifulSoup(html, "html.parser")
        links = [link.get('href') for link in soup.find_all('a') if link.get('href')]

        logger.info("Found links %s", links)
        matching_links = [link for link in links if link.endswith(filename)]
        if not matching_links:
            raise Exception(f'Unable to find link to {filename}')
        if len(matching_links) > 1:
            raise Exception(f'Too many links for {filename}')

        return _download(urljoin(url, matching_links[0]))

    def first_file_from_zip(zip_bytes):
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            name = archive.namelist()[0]
            logger.info('Opening file in zip %s', name)
            with archive.open(name, "r") as file:
                yield file, name

    def nested_files_from_zip(zip_bytes):
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            for name in archive.namelist():
                if not name.lower().startswith(base_filename):
                    # Some sesx16 files seem to contain unrelated data
                    logger.info('Skipping file %s', name)
                    continue
                logger.info('Opening file in zip %s', name)
                with archive.open(name, "r") as file:
                    with zipfile.ZipFile(file) as inner_archive:
                        inner_name = inner_archive.namelist()[0]
                        logger.info('Opening inner file in zip %s', inner_name)
                        with inner_archive.open(inner_name, "r") as inner_file:
                            logger.info('Opened inner file in zip %s', inner_name)
                            yield inner_file, inner_name

    def get_files():
        for year in previous_years:
            yield from nested_files_from_zip(
                get_file_linked_from(
                    config.HMRC_UKTRADEINFO_ARCHIVE_URL,
                    f"/{base_filename}_{year}archive.zip",
                ),
            )

        if latest_file_date.month > 1:
            yield from nested_files_from_zip(
                get_file_linked_from(
                    config.HMRC_UKTRADEINFO_LATEST_URL,
                    f"/{base_filename}_{latest_file_date:%Y}archive.zip",
                ),
            )

        yield from first_file_from_zip(
            get_file_linked_from(
                config.HMRC_UKTRADEINFO_LATEST_URL,
                f"/{base_filename}{latest_file_date:%y%m}.zip",
            ),
        )

    def get_lines(files):
        for file, source_name in files:
            logger.info('Parsing file %s', file)
            for line in _without_first_and_last(file):
                data = line.strip().decode('utf-8').split("|")
                if len(data) == num_expected_fields:
                    yield line.strip().decode('utf-8').split("|") + [source_name]
                else:
                    logger.warn(
                        "Ignoring row with %s fields instead of expected %s: %s",
                        len(data),
                        num_expected_fields,
                        line,
                    )

    def paginate(lines):
        page = []
        for line in lines:
            page.append(line)
            if len(page) == num_per_page:
                yield page
                page = []
        if page:
            yield page

    files = get_files()
    lines = get_lines(files)
    pages = paginate(lines)
    for i, page in enumerate(pages):
        output_filename = f"{i:010}.json"
        logger.info('Saving file to S3 %s', output_filename)
        s3.write_key(output_filename, page)
        logging.info("Fetched from source to %s", output_filename)


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download(source_url):
    logger.info('Downloading %s', source_url)
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error("Request failed: %s", response.text)
        raise

    return response.content


def _without_first_and_last(items):
    try:
        next(items)
    except StopIteration:
        return

    try:
        previous = next(items)
    except StopIteration:
        return

    for item in items:
        yield previous
        previous = item
