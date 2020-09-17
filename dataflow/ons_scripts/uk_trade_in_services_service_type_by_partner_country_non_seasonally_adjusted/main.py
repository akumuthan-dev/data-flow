"""
Lifted and lightly modified (but heavily condensed) from:
https://github.com/GSS-Cogs/family-trade/blob/fca3be954d0b4ed216a4a0989182b19a5c58b23c/datasets/ONS-UK-SA-Trade-in-goods/main.py
"""

from datetime import datetime
from typing import Tuple, Optional

import gssutils
import pandas
import re

import requests
from gssutils import Excel

DATASET_URL = 'https://www.ons.gov.uk/businessindustryandtrade/internationaltrade/datasets/uktradeinservicesservicetypebypartnercountrynonseasonallyadjusted'


def process_data() -> pandas.DataFrame:
    print(datetime.now(), 'process_data start')

    YEAR_RE = re.compile(r'[0-9]{4}')
    YEAR_MONTH_RE = re.compile(
        r'([0-9]{4})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)'
    )
    YEAR_QUARTER_RE = re.compile(r'([0-9]{4})(Q[1-4])')

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
            return f"year/{t}"
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
            return f"month/{year}-{month_num}"
        elif gre.fullmatch(YEAR_QUARTER_RE, t):
            year, quarter = gre.last_match.groups()
            return f"quarter/{year}-{quarter}"
        else:
            print(f"no match for {t}")

    def user_perc(x):
        if str(x) == '-':
            return 'itis-nil'
        elif str(x) == '..':
            return 'disclosive'
        else:
            return None

    print(datetime.now(), "loading dataframe")

    scraper = gssutils.Scraper(DATASET_URL, session=requests.Session())
    dist = scraper.distribution(latest=True, mediaType=Excel)
    tab = dist.as_pandas(sheet_name='Time Series')

    tab = tab.iloc[1:, :]

    tab.columns.values[0] = 'Flow'
    tab.columns.values[1] = 'Trade Services Code'
    tab.columns.values[2] = 'Trade Services Name'
    tab.columns.values[3] = 'Geography Code'
    tab.columns.values[4] = 'Geography Name'

    print(datetime.now(), "melting dataframe")

    new_table = pandas.melt(
        tab,
        id_vars=[
            'Flow',
            'Trade Services Code',
            'Trade Services Name',
            'Geography Code',
            'Geography Name',
        ],
        var_name='Period',
        value_name='Value',
    )

    print(datetime.now(), "cleaning dataframe")

    new_table['Trade Services Code'] = new_table['Trade Services Code'].astype(str)

    new_table['Period'] = new_table['Period'].astype(str)
    new_table['Period'] = new_table['Period'].astype('category')

    new_table['Period Type'] = new_table['Period'].apply(time2periodtype)
    new_table['Period Type'] = new_table['Period Type'].astype('category')

    new_table['Period'] = new_table['Period'].apply(time2period)
    new_table['Period'] = new_table['Period'].astype('category')

    new_table['Flow'] = new_table['Flow'].map(lambda s: s.lower().strip())

    new_table['Seasonal Adjustment'] = 'NSA'
    new_table['Measure Type'] = 'GBP Total'
    new_table['Unit'] = 'gbp-million'

    new_table['Marker'] = new_table.apply(lambda row: user_perc(row['Value']), axis=1)

    new_table['Value'] = pandas.to_numeric(new_table['Value'], errors='coerce')

    indexNames = new_table[
        new_table['Trade Services Code'].str.contains('nan', na=True)
    ].index
    new_table.drop(indexNames, inplace=True)

    print(datetime.now(), "preparing final dataframe")
    new_table = new_table[
        [
            'Geography Code',
            'Geography Name',
            'Period',
            'Period Type',
            'Flow',
            'Trade Services Code',
            'Trade Services Name',
            'Measure Type',
            'Value',
            'Unit',
            'Marker',
        ]
    ]
    new_table.rename(
        columns={
            'Geography Code': 'ons_iso_alpha_2_code',
            'Geography Name': 'ons_region_name',
            'Period': "period",
            'Period Type': "period_type",
            'Flow': "direction",
            'Trade Services Code': "product_code",
            'Trade Services Name': "product_name",
            'Measure Type': "measure_type",
            'Value': "value",
            'Unit': "unit",
            'Marker': "marker",
        },
        inplace=True,
    )

    print(datetime.now(), f'process_data finished', len(new_table))

    return new_table


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
