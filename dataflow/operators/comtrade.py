import codecs
import csv
import itertools
import time
import json
import logging

import backoff
import requests

from dataflow import config
from dataflow.utils import S3Data, logger


def fetch_comtrade_goods_data(
    table_name: str, ts_nodash: str, **kwargs,
):
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
        '2nd Partner Code',
        '2nd Partner',
        '2nd Partner ISO',
        'Customs Proc. Code',
        'Customs',
        'Mode of Transport Code',
        'Mode of Transport',
        'Commodity Code',
        'Commodity',
        'Qty Unit Code',
        'Qty Unit',
        'Qty',
        'Alt Qty Unit Code',
        'Alt Qty Unit',
        'Alt Qty',
        'Netweight (kg)',
        'Gross weight (kg)',
        'Trade Value (US$)',
        'CIF Trade Value (US$)',
        'FOB Trade Value (US$)',
        'Flag',
    ]
    s3 = S3Data(table_name, ts_nodash)

    with _download('https://comtrade.un.org/Data/cache/years.json') as response:
        years_objs = json.loads(response.content)
    if years_objs['more']:
        raise Exception('Unable to fetch all years')

    with _download('https://comtrade.un.org/Data/cache/reporterAreas.json') as response:
        reporter_areas_objs = json.loads(response.content)
    if reporter_areas_objs['more']:
        raise Exception('Unable to fetch all reporter areas')

    years = [
        year_obj['id']
        for year_obj in years_objs['results']
        if year_obj['id'].lower() != 'all'
    ]
    reporter_areas = [
        reporter_area['id']
        for reporter_area in reporter_areas_objs['results']
        if reporter_area['id'].lower() != 'all'
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

    def download(period_and_partner_area_pages):
        type_goods = 'C'
        frequency_annual = 'A'
        classification_hs = 'HS'
        partner_areas_all = 'all'
        trade_regime_all = 'all'
        desired_commodity_codes_total = 'ALL'

        for period_page, reporter_page in period_and_partner_area_pages:
            for row in _download_dicts(
                'https://comtrade.un.org/api/get',
                (
                    ('max', '100000'),
                    ('fmt', 'csv'),
                    ('type', type_goods),
                    ('freq', frequency_annual),
                    ('px', classification_hs),
                    ('ps', ','.join(period_page)),
                    ('r', ','.join(reporter_page)),
                    ('p', partner_areas_all),
                    ('rg', trade_regime_all),
                    ('cc', desired_commodity_codes_total),
                    ('token', config.COMTRADE_TOKEN),
                ),
            ):
                if list(row.keys()) != expected_keys:
                    raise Exception('Unexpected columns {}'.format(row.keys()))
                yield list(row.values())

    period_pages = paginate(years, 5)
    partner_area_pages = paginate(reporter_areas, 5)
    period_and_partner_area_pages = itertools.product(period_pages, partner_area_pages)
    result_records = download(period_and_partner_area_pages)
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


def throttled_generator(generator_func):
    ''' Forces an interval of 1 second between the completion of every all to the generator '''

    seconds_between_calls = 1.0
    previous = float('-infinity')

    def _throttled_func(*args, **kwargs):
        nonlocal previous

        now = time.monotonic()
        to_sleep = max(0, seconds_between_calls - (now - previous))
        logger.info('Sleeping for %s seconds', to_sleep)
        time.sleep(to_sleep)

        yield from generator_func(*args, **kwargs)

        previous = time.monotonic()

    return _throttled_func


@throttled_generator
def _download_dicts(source_url, params=()):
    with _download(source_url, params=params) as response:
        yield from csv.DictReader(codecs.iterdecode(response.iter_lines(), 'utf-8'))
