import codecs
import csv
import io
import json
import logging
import zipfile

import backoff
import requests

from dataflow import config
from dataflow.utils import S3Data, logger


def fetch_comtrade_goods_data(
    table_name: str, ts_nodash: str, **kwargs,
):
    s3 = S3Data(table_name, ts_nodash)
    expected_keys = [
        'Classification',
        'Year',
        'Period',
        'Period Desc.',
        'Aggregate Level',
        'Is Leaf Code',
        'Trade Flow Code',
        'Trade Flow',
        'Reporter Code',
        'Reporter',
        'Reporter ISO',
        'Partner Code',
        'Partner',
        'Partner ISO',
        'Commodity Code',
        'Commodity',
        'Qty Unit Code',
        'Qty Unit',
        'Qty',
        'Netweight (kg)',
        'Trade Value (US$)',
        'Flag',
    ]
    _fetch(s3, 'C', expected_keys)


def fetch_comtrade_services_data(
    table_name: str, ts_nodash: str, **kwargs,
):
    s3 = S3Data(table_name, ts_nodash)
    expected_keys = [
        'Classification',
        'Year',
        'Period',
        'Period Desc.',
        'Aggregate Level',
        'Is Leaf Code',
        'Trade Flow Code',
        'Trade Flow',
        'Reporter Code',
        'Reporter',
        'Reporter ISO',
        'Partner Code',
        'Partner',
        'Partner ISO',
        'Commodity Code',
        'Commodity',
        'Trade Value (US$)',
        'Flag',
    ]
    _fetch(s3, 'S', expected_keys)


def _fetch(s3, trade_type, expected_keys):
    with _download('https://comtrade.un.org/Data/cache/years.json') as response:
        years_objs = json.loads(response.content)
    if years_objs['more']:
        raise Exception('Unable to fetch all years')

    years = [
        year_obj['id']
        for year_obj in years_objs['results']
        if year_obj['id'].lower() != 'all'
    ]

    def paginate(items, num_per_page):
        page = []
        for item in items:
            page.append(item)
            if len(page) == num_per_page:
                yield page
                page = []
        if page:
            yield page

    def file_from_zip(zip_bytes):
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            name = archive.namelist()[0]
            logger.info('Opening csv file %s in zip', name)
            with archive.open(name, "r") as file:
                yield from file

    def get_files(trade_type, expected_keys, periods):
        frequency = 'A'
        classification = 'HS' if trade_type == 'C' else 'EB02'
        for period in periods:
            yield from file_from_zip(
                _download(
                    f'https://comtrade.un.org/api/get/bulk/{trade_type}/{frequency}/{period}/all/{classification}',
                    params=(('token', config.COMTRADE_TOKEN),),
                ).content
            )

    def get_dicts(f):
        for row in csv.DictReader(codecs.iterdecode(f, 'utf-8-sig')):
            if list(row.keys()) != expected_keys:
                raise Exception('Unexpected columns {}'.format(row.keys()))
            yield {k: v if v else None for k, v in row.items()}

    files = get_files(trade_type, expected_keys, years)
    result_records = get_dicts(files)
    results_pages = paginate(result_records, 10000)

    for i, page in enumerate(results_pages):
        output_filename = f"{i:010}.json"
        logger.info('Saving file to S3 %s', output_filename)
        s3.write_key(output_filename, page)
        logging.info("Fetched from source to %s", output_filename)


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=20)
def _download(source_url, params=()):
    logger.info(
        'Downloading %s %s',
        source_url,
        [(key, value) for (key, value) in params if key != 'token'],
    )
    response = requests.get(source_url, stream=True, params=params)
    response.raise_for_status()
    return response
