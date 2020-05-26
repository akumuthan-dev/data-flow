# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # ONS Trade in goods: country-by-commodity, imports and exports
#
# This data is split into two distributions, one for imports and the other for exports:
#
# https://www.ons.gov.uk/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityimports
#
# and
#
# https://www.ons.gov.uk/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityexports   

from gssutils import *
import numpy as np


# +
def run_script(s):
    get_ipython().run_line_magic("run", s)
    return table

observations = pd.concat(
    run_script(s) for s in ['exports', 'imports']
).drop_duplicates()

# +
# observations.rename(columns={'Flow': 'Flow Directions'}, inplace=True)

#Flow has been changed to Flow Direction to differentiate from Migration Flow dimension
# -

out = Path('out')
out.mkdir(exist_ok=True)

for i, segment in enumerate(np.array_split(observations, 100)):
    segment.to_csv(out / f'observations-{i:010}.csv', index=False)
