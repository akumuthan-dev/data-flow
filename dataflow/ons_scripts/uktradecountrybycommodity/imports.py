#!/usr/bin/env python
# coding: utf-8
# %% [markdown]
# # Country by commodity imports

# %%
from gssutils import *
import json

landingPage = json.load(open('info.json'))['landingPage'][1]
display(landingPage)

scraper = Scraper(landingPage)
scraper


# %%
from zipfile import ZipFile
from io import BytesIO

distribution = scraper.distribution(mediaType=lambda x: 'zip' in x, latest=True)

with ZipFile(BytesIO(scraper.session.get(distribution.downloadURL).content)) as zip:
    assert(len(zip.namelist()) == 1)
    with zip.open(zip.namelist()[0]) as excelFile:
        buffered_fobj = BytesIO(excelFile.read())
        data = pd.read_excel(buffered_fobj,
                             sheet_name=1, dtype={
                                 'COMMODITY': 'category',
                                 'COUNTRY': 'category',
                                 'DIRECTION': 'category'
                             }, na_values=['','N/A'], keep_default_na=False)
data

# %%
table = data.drop(columns='DIRECTION')
table.rename(columns={
    'COMMODITY': 'Product',
    'COUNTRY': 'Geography'}, inplace=True)
table = pd.melt(table, id_vars=['Product','Geography'], var_name='Period', value_name='Value')
table['Period'] = table['Period'].astype('category')
# table.dropna(subset=['Value'], inplace=True)
# table['Value'] = table['Value'].astype(int)
table


# %%
product = table['Product'].str.split(' ', n=1, expand=True)
table['Product Code'], table['Product Name'] = product[0].astype('category'), product[1].astype('category')

geography = table['Geography'].str.split(' ', n=1, expand=True)
table['Geography Code'], table['Geography Name'] = geography[0].astype('category'), geography[1].astype('category')

table.drop(columns=['Product', 'Geography'], inplace=True)


# %%
import re
YEAR_RE = re.compile(r'[0-9]{4}')
YEAR_MONTH_RE = re.compile(r'([0-9]{4})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)')
YEAR_QUARTER_RE = re.compile(r'([0-9]{4})\s+(Q[1-4])')

# from https://stackoverflow.com/questions/597476/how-to-concisely-cascade-through-multiple-regex-statements-in-python
class Re(object):
  def __init__(self):
    self.last_match = None
  def fullmatch(self,pattern,text):
    self.last_match = re.fullmatch(pattern,text)
    return self.last_match

def time2period(t):
    gre = Re()
    if gre.fullmatch(YEAR_RE, t):
        return f"year/{t}"
    elif gre.fullmatch(YEAR_MONTH_RE, t):
        year, month = gre.last_match.groups()
        month_num = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
                     'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}.get(month)
        return f"month/{year}-{month_num}"
    elif gre.fullmatch(YEAR_QUARTER_RE, t):
        year, quarter = gre.last_match.groups()
        return f"quarter/{year}-{quarter}"
    else:
        print(f"no match for {t}")

table['Period'].cat.categories = table['Period'].cat.categories.map(time2period)


# %%
table['Seasonal Adjustment'] = pd.Series('NSA', index=table.index, dtype='category')
table['Measure Type'] = pd.Series('gbp-total', index=table.index, dtype='category')
table['Unit'] = pd.Series('gbp', index=table.index, dtype='category')
table['Flow'] = pd.Series('imports', index=table.index, dtype='category')


# %%

def user_perc(x):
    if pd.isna(x):
        return 'N/A'
    elif type(x) == str:
        return x
    else:
        return ''

table['Marker'] = table.apply(lambda row: user_perc(row['Value']), axis=1)
table['Marker'] = table['Marker'].astype('category')
table['Value'] = pd.to_numeric(table['Value'], errors='coerce')


# %%
table = table[['Geography Code', 'Geography Name', 'Period', 'Flow', 'Product Code', 'Product Name', 'Seasonal Adjustment', 'Measure Type', 'Value', 'Unit', 'Marker']]
table

