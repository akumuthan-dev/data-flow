import codecs
import csv
import io
import json
import zipfile
from typing import TYPE_CHECKING, Generator

import backoff
import pandas
import requests

from dataflow import config
from dataflow.utils import S3Data, logger

if TYPE_CHECKING:
    from dataflow.dags.comtrade_pipelines import ComtradeGoodsPipeline  # noqa


def fetch_comtrade_goods_data_frames() -> Generator[pandas.DataFrame, None, None]:
    expected_columns_and_dtypes = {
        'Classification': 'category',
        'Year': 'uint16',
        'Period': 'uint16',
        'Period Desc.': 'uint16',
        'Aggregate Level': 'uint8',
        'Is Leaf Code': 'bool',
        'Trade Flow Code': 'int',
        'Trade Flow': 'category',
        'Reporter Code': 'uint16',
        'Reporter': 'category',
        'Reporter ISO': 'category',
        'Partner Code': 'category',
        'Partner': 'category',
        'Partner ISO': 'category',
        'Commodity Code': 'category',
        'Commodity': 'category',
        'Qty Unit Code': 'int',
        'Qty Unit': 'category',
        'Qty': 'float',
        'Trade Value (US$)': 'int64',
    }

    trade_type = 'C'

    years = _get_years(trade_type)
    zipfile_bytes_for_each_period = _get_zipfile_bytes_for_each_period(
        trade_type, years, config.COMTRADE_TOKEN
    )
    csv_files_for_each_period = _get_csvfile_for_each_period(
        zipfile_bytes_for_each_period
    )
    for csv_file in csv_files_for_each_period:
        df_chunk_generator = pandas.read_csv(
            csv_file,
            chunksize=1_000_000,
            dtype=expected_columns_and_dtypes,
            usecols=[c for c in expected_columns_and_dtypes.keys()],
        )
        for df_chunk in df_chunk_generator:
            yield df_chunk


def _get_years(trade_type):
    with _download('https://comtrade.un.org/Data/cache/years.json') as response:
        years_objs = json.loads(response.content)
    if years_objs['more']:
        raise Exception('Unable to fetch all years')

    years = [
        year_obj['id']
        for year_obj in years_objs['results']
        if year_obj['id'].lower() != 'all'
    ]

    if trade_type == 'S':
        # Comtrade API returns 404 for years before 2000 for services (HS) files, so we will just skip those years
        # in this case in order to get back to a functioning pipeline.
        years = filter(lambda o: int(o) >= 2000, years)
    elif trade_type == 'C':
        # Comtrade API returns 404 for years before 1988 for goods (C) files, so we will just skip those years
        # in this case in order to get back to a functioning pipeline.
        years = filter(lambda o: int(o) >= 1988, years)

    return list(years)


def _get_csvfile_for_each_period(data_for_each_period):
    for zip_bytes in data_for_each_period:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            name = archive.namelist()[0]
            logger.info('Opening csv file %s in zip', name)
            with archive.open(name, "r") as file:
                yield file


def _get_zipfile_bytes_for_each_period(trade_type, periods, comtrade_token):
    # There is potential for a fairly "quick win" optimisation here. This comtrade API endpoint serves files quite
    # slowly - sometimes taking 3+ minutes. Multiplied by 30+ files, this adds a non-negligible delay to the pipeline
    # as a whole. If we could pull the next file in the background so that it's always ready as soon as the processing
    # steps want it, we could eliminate pretty much all of the downtime (except for the first file). Scraping the API
    # in a background thread and pushing results onto a queue with a maxsize of 1 seems like a fairly trivial
    # candidate implementation. Not doing it yet because the current speed should be "good enough".
    frequency = 'A'
    classification = 'HS' if trade_type == 'C' else 'EB02'
    for period in periods:
        yield _download(
            f'https://comtrade.un.org/api/get/bulk/{trade_type}/{frequency}/{period}/all/{classification}',
            params=(('token', comtrade_token),),
        ).content


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
    years = _get_years(trade_type)

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
            yield file_from_zip(
                _download(
                    f'https://comtrade.un.org/api/get/bulk/{trade_type}/{frequency}/{period}/all/{classification}',
                    params=(('token', config.COMTRADE_TOKEN),),
                ).content
            )

    def get_dicts(f):
        for f in files:
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


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=10)
def _download(source_url, params=()):
    logger.info(
        'Downloading %s %s',
        source_url,
        [(key, value) for (key, value) in params if key != 'token'],
    )
    response = requests.get(source_url, stream=True, params=params)
    response.raise_for_status()
    return response
