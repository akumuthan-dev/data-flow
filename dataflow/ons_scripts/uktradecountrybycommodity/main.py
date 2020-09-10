"""
Lifted and lightly modified (but heavily condensed) from:
https://github.com/GSS-Cogs/family-trade/tree/fca3be954d0b4ed216a4a0989182b19a5c58b23c/datasets/ONS-Trade-in-goods
"""


import re
from datetime import datetime, date
from io import BytesIO
from multiprocessing import Pool
from zipfile import ZipFile

import numpy
from gssutils import Scraper
import pandas as pd
from pandas import DataFrame


EXPORTS_DATASET_URL = "https://www.ons.gov.uk/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityexports"
IMPORTS_DATASET_URL = "https://www.ons.gov.uk/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityimports"


def process_data(flow_type, dataset_url):
    YEAR_MONTH_RE = re.compile(
        r'([0-9]{4})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)'
    )

    # from https://stackoverflow.com/questions/597476/how-to-concisely-cascade-through-multiple-regex-statements-in-python
    class Re(object):
        def __init__(self):
            self.last_match = None

        def fullmatch(self, pattern, text):
            self.last_match = re.fullmatch(pattern, text)
            return self.last_match

    def time2period(t):
        gre = Re()
        if gre.fullmatch(YEAR_MONTH_RE, t):
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
        else:
            print(f"no match for {t}")

    print(datetime.now(), f'process_data({flow_type}, {dataset_url}) start')
    scraper = Scraper(dataset_url)
    distribution = scraper.distribution(mediaType=lambda x: 'zip' in x, latest=True)

    with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
        assert len(zip.namelist()) == 1
        with zip.open(zip.namelist()[0]) as excelFile:
            table = pd.read_excel(
                excelFile,
                sheet_name=1,
                dtype={
                    'COMMODITY': 'category',
                    'COUNTRY': 'category',
                    'DIRECTION': 'category',
                },
                na_values=['', 'N/A'],
                keep_default_na=False,
            )

    print(datetime.now(), flow_type, "loaded dataframe")
    table.drop(columns='DIRECTION', inplace=True)
    table.rename(columns={'COMMODITY': 'Product', 'COUNTRY': 'Geography'}, inplace=True)
    table = pd.melt(
        table, id_vars=['Product', 'Geography'], var_name='Period', value_name='Value',
    )
    print(datetime.now(), flow_type, "melted")
    table['Period'] = table['Period'].astype('category')
    product = table['Product'].str.split(' ', n=1, expand=True)
    table['Product Code'], table['Product Name'] = (
        product[0].astype('category'),
        product[1].astype('category'),
    )
    geography = table['Geography'].str.split(' ', n=1, expand=True)
    table['Geography Code'], table['Geography Name'] = (
        geography[0].astype('category'),
        geography[1].astype('category'),
    )
    table.drop(columns=['Product', 'Geography'], inplace=True)
    print(datetime.now(), flow_type, "dropped product+geography")

    table['Period'].cat.categories = table['Period'].cat.categories.map(time2period)
    table['Period Type'] = 'month'
    table['Period Type'] = table['Period Type'].astype('category')

    table['Seasonal Adjustment'] = pd.Series('NSA', index=table.index, dtype='category')
    table['Measure Type'] = pd.Series('gbp-total', index=table.index, dtype='category')
    table['Unit'] = pd.Series('gbp', index=table.index, dtype='category')
    table['Flow'] = pd.Series(flow_type, index=table.index, dtype='category')

    print(datetime.now(), flow_type, "starting apply")
    table['Marker'] = numpy.select(
        condlist=[table['Value'].isna(), table['Value'].dtype == 'str'],
        choicelist=['not-available', table['Value']],
        default='',
    )
    print(datetime.now(), flow_type, "finished apply")

    table['Marker'] = table['Marker'].astype('category')
    table['Value'] = pd.to_numeric(table['Value'], errors='coerce')
    table = table[
        [
            'Geography Code',
            'Geography Name',
            'Period',
            'Period Type',
            'Flow',
            'Product Code',
            'Product Name',
            'Seasonal Adjustment',
            'Measure Type',
            'Value',
            'Unit',
            'Marker',
        ]
    ]
    table.rename(
        columns={
            'Geography Code': 'ons_iso_alpha_2_code',
            'Geography Name': 'ons_region_name',
            'Period': "period",
            'Period Type': "period_type",
            'Flow': "direction",
            'Product Code': "product_code",
            'Product Name': "product_name",
            'Seasonal Adjustment': "seasonal_adjustment",
            'Measure Type': "measure_type",
            'Value': "total",
            'Unit': "unit",
            'Marker': "marker",
        },
        inplace=True,
    )
    print(
        datetime.now(), f'process_data({flow_type}, {dataset_url}) finished', len(table)
    )
    return table


def get_source_data_modified_date() -> datetime:
    exports_scraper = Scraper(EXPORTS_DATASET_URL)
    imports_scraper = Scraper(IMPORTS_DATASET_URL)

    oldest_date: date = min(
        exports_scraper.dataset.issued, imports_scraper.dataset.issued
    )

    return datetime(
        year=oldest_date.year,
        month=oldest_date.month,
        day=oldest_date.day,
        hour=0,
        minute=0,
        second=0,
    )


def get_data() -> DataFrame:
    with Pool(processes=2) as pool:
        print('start getting', datetime.now())
        exports = pool.apply_async(
            process_data,
            kwds=dict(flow_type='exports', dataset_url=EXPORTS_DATASET_URL),
        )
        imports = pool.apply_async(
            process_data,
            kwds=dict(flow_type='imports', dataset_url=IMPORTS_DATASET_URL),
        )
        df = pd.concat(
            [exports.get(timeout=24 * 60 * 60), imports.get(timeout=24 * 60 * 60),]
        ).drop_duplicates()
        print('end getting', datetime.now(), len(df))

    return df
