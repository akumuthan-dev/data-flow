"""
Lifted and lightly modified (but heavily condensed) from:
https://github.com/GSS-Cogs/family-trade/blob/fca3be954d0b4ed216a4a0989182b19a5c58b23c/datasets/ONS-UK-Total-trade/main.py
"""
import json
from datetime import datetime
from typing import Tuple, Optional

import gssutils
import pandas
import re

import requests
from gssutils import (
    Scraper,
    DOWN,
    RIGHT,
    HDim,
    HDimConst,
    CLOSEST,
    ABOVE,
    DIRECTLY,
    LEFT,
    ConversionSegment,
)

DATASET_URL = "https://www.ons.gov.uk/businessindustryandtrade/internationaltrade/datasets/uktotaltradeallcountriesnonseasonallyadjusted"


def process_data():
    print(datetime.now(), 'process_data start')

    YEAR_RE = re.compile(r'[0-9]{4}')
    YEAR_MONTH_RE = re.compile(
        r'([0-9]{4})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)'
    )
    YEAR_QUARTER_RE = re.compile(r'([0-9]{4})(Q[1-4])')

    def product(name):
        if 'Total Trade' in name:
            return 'goods-and-services'
        elif 'TiG' in name:
            return 'goods'
        elif 'TiS' in name:
            return 'services'
        raise ValueError(f'Unknown product type ${name}')

    class Re(object):
        def __init__(self):
            self.last_match = None

        def fullmatch(self, pattern, text):
            self.last_match = re.fullmatch(pattern, text)
            return self.last_match

    def time2periodtype(t):
        gre = Re()
        if gre.fullmatch(YEAR_RE, t):
            return "year"
        elif gre.fullmatch(YEAR_MONTH_RE, t):
            return "month"
        elif gre.fullmatch(YEAR_QUARTER_RE, t):
            return "quarter"
        else:
            print(f"no match for {t}")

    def time2period(t):
        gre = Re()
        if gre.fullmatch(YEAR_RE, t):
            return t
        elif gre.fullmatch(YEAR_MONTH_RE, t):
            year, month = gre.last_match.groups()
            month_num = {
                'JAN': '01',
                'FEB': '02',
                'MAR': '03',
                'APR': '04',
                'MAY': '05',
                'JUN': '06',
                'JUL': '07',
                'AUG': '08',
                'SEP': '09',
                'OCT': '10',
                'NOV': '11',
                'DEC': '12',
            }.get(month)
            return f"{year}-{month_num}"
        elif gre.fullmatch(YEAR_QUARTER_RE, t):
            year, quarter = gre.last_match.groups()
            return f"{year}-{quarter}"
        else:
            print(f"no match for {t}")

    def process_sheet(sheetname, tab) -> pandas.DataFrame:
        print(datetime.now(), f'spreadsheet scrape - {sheetname} - start')

        if 'Index' in sheetname or 'Contact Sheet' in sheetname:
            print(datetime.now(), f"skipping {sheetname}")
            return pandas.DataFrame()

        observations = (
            tab.excel_ref('C7')
            .expand(DOWN)
            .expand(RIGHT)
            .is_not_blank()
            .is_not_whitespace()
        )
        Year = tab.excel_ref('C4').expand(RIGHT).is_not_blank().is_not_whitespace()
        Flow = tab.fill(DOWN).one_of(['Exports', 'Imports'])
        geo = tab.excel_ref('A7').expand(DOWN).is_not_blank().is_not_whitespace()
        geo_name = tab.excel_ref('B7').expand(DOWN).is_not_blank().is_not_whitespace()
        Dimensions = [
            HDim(Year, 'Period', DIRECTLY, ABOVE),
            HDim(geo, 'Geography Code', DIRECTLY, LEFT),
            HDim(geo_name, 'Geography Name', DIRECTLY, LEFT),
            HDim(Flow, 'Flow', CLOSEST, ABOVE),
            HDimConst('Measure Type', 'gbp-total'),
            HDimConst('Unit', 'gbp-million'),
        ]
        c1 = ConversionSegment(observations, Dimensions, processTIMEUNIT=True)
        new_table = c1.topandas()

        new_table.rename(columns={'OBS': 'Value', 'DATAMARKER': 'Marker'}, inplace=True)
        new_table['Flow'] = new_table['Flow'].map(lambda s: s.lower().strip())
        new_table['Product'] = product(sheetname)
        new_table['Period'] = new_table['Period'].astype(str)
        new_table['Marker'] = (
            new_table['Marker'].fillna('') if 'Marker' in new_table else ''
        )
        new_table = new_table[
            [
                'Geography Code',
                'Geography Name',
                'Period',
                'Flow',
                'Product',
                'Measure Type',
                'Value',
                'Unit',
                'Marker',
            ]
        ]
        print(datetime.now(), f'scrape {sheetname} - complete')
        return new_table

    print(datetime.now(), "gathering info")
    scraper = Scraper(DATASET_URL)
    tabs = {tab.name: tab for tab in scraper.distribution(latest=True).as_databaker()}

    print(datetime.now(), f'spreadsheet scrape - start')
    table = pandas.concat([process_sheet(*args) for args in tabs.items()])
    print(datetime.now(), f'spreadsheet scrape - complete')

    table['Period'] = table.Period.str.replace('\.0', '')

    table['Period Type'] = table['Period'].apply(time2periodtype)
    table['Period Type'] = table['Period Type'].astype('category')

    table['Period'] = table['Period'].apply(time2period)
    table['Period'] = table['Period'].astype('category')
    table['Period Type'] = table['Period Type'].astype('category')

    # (pandas) "Int64" type allows null values, unlike (numpy) "int64" - yes, the case matters.
    table['Value'] = pandas.to_numeric(table['Value'], errors='coerce').astype('Int64')

    print(datetime.now(), "dropping duplicates")
    table = table.drop_duplicates()

    print(datetime.now(), "renaming columns")
    table.rename(
        columns={
            'Geography Code': 'ons_iso_alpha_2_code',
            'Geography Name': 'ons_region_name',
            'Period': "period",
            'Period Type': "period_type",
            'Flow': "direction",
            'Product': "product_name",
            'Measure Type': "measure_type",
            'Value': "value",
            'Unit': "unit",
            'Marker': "marker",
        },
        inplace=True,
    )

    print(datetime.now(), f'process_data finished', len(table))

    return table


def get_current_and_next_release_date() -> Tuple[datetime, Optional[datetime]]:
    scraper = gssutils.Scraper(DATASET_URL, session=requests.Session())

    try:
        # The date of the next release may not be known.
        next_release_date_utc = scraper.dataset.updateDueOn
    except AttributeError:
        next_release_date_utc = None

    return (
        datetime(
            year=scraper.dataset.issued.year,
            month=scraper.dataset.issued.month,
            day=scraper.dataset.issued.day,
            hour=0,
            minute=0,
            second=0,
        ),
        next_release_date_utc,
    )


def get_data() -> pandas.DataFrame:
    print(datetime.now(), 'start getting')

    df = process_data()

    print(datetime.now(), 'end getting', len(df))

    return df
