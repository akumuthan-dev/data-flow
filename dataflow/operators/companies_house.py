import codecs
import csv
import io
import json
import zipfile
from datetime import datetime
import backoff
import requests

from dataflow import config
from dataflow.utils import S3Data, logger


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download(source_url):
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error("Request failed: %s", response.text)
        raise

    return response.content


def fetch_companies_house_companies(
    table_name: str,
    source_url: str,
    number_of_files: int,
    page_size: int = 10000,
    **kwargs,
):
    """
    Loop through `number_of_files`, build the url, download the zip file,
    extract and write data in batches of `page_size` to s3
    """
    s3 = S3Data(table_name, kwargs['ts_nodash'])
    page = 1
    results = []
    publish_date = datetime(
        kwargs['next_execution_date'].year, kwargs['next_execution_date'].month, 1
    ).strftime('%Y-%m-01')
    for file_num in range(1, number_of_files + 1):
        url = source_url.format(
            file_date=publish_date, file_num=file_num, num_files=number_of_files,
        )
        logger.info('Fetching zip file from %s', url)
        with zipfile.ZipFile(io.BytesIO(_download(url))) as archive:
            with archive.open(archive.namelist()[0], 'r') as f:
                reader = csv.DictReader(codecs.iterdecode(f, 'utf-8'))
                if reader.fieldnames is not None:
                    reader.fieldnames = [x.strip() for x in reader.fieldnames]
                for row in reader:
                    row['publish_date'] = publish_date
                    results.append(row)
                    if len(results) >= page_size:
                        s3.write_key(f'{page:010}.json', results)
                        results = []
                        page += 1

    if results:
        s3.write_key(f'{page:010}.json', results)

    logger.info('Fetching from source completed')


def fetch_companies_house_significant_persons(
    table_name: str, source_url: str, **kwargs,
):
    s3 = S3Data(table_name, kwargs['ts_nodash'])
    page_size = 10000
    page = 1
    results = []
    for file in range(1, config.COMPANIES_HOUSE_PSC_TOTAL_FILES + 1):
        url = source_url.format(file=file, total=config.COMPANIES_HOUSE_PSC_TOTAL_FILES)
        logger.info('Fetching zip file from %s', url)
        with zipfile.ZipFile(io.BytesIO(_download(url))) as archive:
            with archive.open(archive.namelist()[0], 'r') as f:
                for line in f:
                    results.append(json.loads(line))
                    if len(results) >= page_size:
                        s3.write_key(f'{page:010}.json', results)
                        results = []
                        page += 1
    if results:
        s3.write_key(f'{page:010}.json', results)
    logger.info("Fetching from source completed")
