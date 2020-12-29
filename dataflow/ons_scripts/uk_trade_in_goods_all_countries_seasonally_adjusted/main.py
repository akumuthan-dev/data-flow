"""
Lifted and lightly modified (but heavily condensed) from:
https://github.com/GSS-Cogs/family-trade/blob/fca3be954d0b4ed216a4a0989182b19a5c58b23c/datasets/ONS-UK-SA-Trade-in-goods/main.py
"""

from datetime import datetime
from typing import Tuple

import gssutils
import pandas
import re

import requests

DATASET_URL = 'https://www.ons.gov.uk/economy/nationalaccounts/balanceofpayments/datasets/uktradeallcountriesseasonallyadjusted'


def process_data():
    print(datetime.now(), 'process_data start')

    YEAR_RE = re.compile(r'[0-9]{4}')
    YEAR_MONTH_RE = re.compile(
        r'([0-9]{4})(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    )

    class Re(object):
        def __init__(self):
            self.last_match = None

        def fullmatch(self, pattern, text):
            self.last_match = re.fullmatch(pattern, text)
            return self.last_match

    def time2periodtype(t):
        gre = Re()
        if gre.fullmatch(YEAR_RE, t):
            return f"year"
        elif gre.fullmatch(YEAR_MONTH_RE, t):
            year, month = gre.last_match.groups()
            month_num = {
                'Jan': '01',
                'Feb': '02',
                'Mar': '03',
                'Apr': '04',
                'May': '05',
                'Jun': '06',
                'Jul': '07',
                'Aug': '08',
                'Sep': '09',
                'Oct': '10',
                'Nov': '11',
                'Dec': '12',
            }.get(month)
            return f"month"
        else:
            print(f"no match for {t}")

    def time2period(t):
        gre = Re()
        if gre.fullmatch(YEAR_RE, t):
            return t
        elif gre.fullmatch(YEAR_MONTH_RE, t):
            year, month = gre.last_match.groups()
            month_num = {
                'Jan': '01',
                'Feb': '02',
                'Mar': '03',
                'Apr': '04',
                'May': '05',
                'Jun': '06',
                'Jul': '07',
                'Aug': '08',
                'Sep': '09',
                'Oct': '10',
                'Nov': '11',
                'Dec': '12',
            }.get(month)
            return f"{year}-{month_num}"
        else:
            print(f"no match for {t}")

    def process_sheet(sheetname, tab):
        print(datetime.now(), f'spreadsheet scrape - {sheetname} - start')
        observations = (
            tab.excel_ref('C7')
            .expand(gssutils.DOWN)
            .expand(gssutils.RIGHT)
            .is_not_blank()
            .is_not_whitespace()
        )
        Year = (
            tab.excel_ref('C5')
            .expand(gssutils.RIGHT)
            .is_not_blank()
            .is_not_whitespace()
        )
        geo_codes = (
            tab.excel_ref('A7').expand(gssutils.DOWN).is_not_blank().is_not_whitespace()
        )
        geo_names = (
            tab.excel_ref('B7').expand(gssutils.DOWN).is_not_blank().is_not_whitespace()
        )
        Dimensions = [
            gssutils.HDim(Year, 'period', gssutils.DIRECTLY, gssutils.ABOVE),
            gssutils.HDim(
                geo_codes, 'ons_iso_alpha_2_code', gssutils.DIRECTLY, gssutils.LEFT,
            ),
            gssutils.HDim(
                geo_names, 'ons_region_name', gssutils.DIRECTLY, gssutils.LEFT,
            ),
            gssutils.HDimConst('measure_type', 'gbp-total'),
            gssutils.HDimConst('unit', 'gbp-million'),
        ]
        c1 = gssutils.ConversionSegment(observations, Dimensions, processTIMEUNIT=True)
        new_table = c1.topandas()
        new_table.rename(columns={"OBS": "value", "DATAMARKER": "marker"}, inplace=True)

        if 'Imports' in sheetname:
            new_table['direction'] = 'imports'
        elif 'Exports' in sheetname:
            new_table['direction'] = 'exports'
        else:
            new_table['direction'] = 'other'

        new_table['period'] = new_table['period'].astype(str)
        new_table = new_table[
            [
                'ons_iso_alpha_2_code',
                'ons_region_name',
                'period',
                'direction',
                'measure_type',
                'value',
                'unit',
                'marker',
            ]
        ]
        print(datetime.now(), f'scrape {sheetname} - complete')
        return new_table

    scraper = gssutils.Scraper(DATASET_URL, session=requests.Session())
    tabs = {tab.name: tab for tab in scraper.distributions[0].as_databaker()}

    print(datetime.now(), f'spreadsheet scrape - start')
    table = pandas.concat([process_sheet(*args) for args in tabs.items()])
    print(datetime.now(), f'spreadsheet scrape - complete')

    table['period'] = table.period.str.replace('\.0', '')

    table['period_type'] = table['period'].apply(time2periodtype)
    table['period_type'] = table['period_type'].astype('category')

    table['period'] = table['period'].apply(time2period)
    table['period'] = table['period'].astype('category')
    table['period_type'] = table['period_type'].astype('category')

    # (pandas) "Int64" type allows null values, unlike (numpy) "int64" - yes, the case matters.
    table['value'] = pandas.to_numeric(table['value'], errors='coerce').astype('Int64')
    table['marker'].replace('N/A', 'not-applicable', inplace=True)

    table = table[
        [
            'ons_iso_alpha_2_code',
            'ons_region_name',
            'period',
            'period_type',
            'direction',
            'measure_type',
            'value',
            'unit',
            'marker',
        ]
    ]

    # Fix some broken data
    table.loc[table['ons_region_name'] == 'Total EU ', 'ons_region_name'] = 'Total EU'
    table.loc[
        table['ons_region_name'] == 'Total Extra EU  (Rest of World)', 'ons_region_name'
    ] = 'Total Extra EU (Rest of World)'

    print(datetime.now(), f'transformed and normalised data')

    return [table.drop_duplicates()]


def get_current_and_next_release_date() -> Tuple[datetime, datetime]:
    scraper = gssutils.Scraper(DATASET_URL, session=requests.Session())

    return (
        datetime(
            year=scraper.dataset.issued.year,
            month=scraper.dataset.issued.month,
            day=scraper.dataset.issued.day,
            hour=0,
            minute=0,
            second=0,
        ),
        scraper.dataset.updateDueOn,
    )


def get_data() -> pandas.DataFrame:
    print(datetime.now(), 'start getting')

    df = process_data()

    print(datetime.now(), 'end getting', len(df))

    return df
