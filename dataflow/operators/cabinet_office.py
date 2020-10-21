from datetime import datetime
from typing import Callable

import pandas as pd

from dataflow.operators.csv_inputs import fetch_mapped_hosted_csvs


def fetch_gender_pay_gap_files(
    table_name: str,
    base_url: str,
    records_start_year: int,
    transform_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    **kwargs
):
    cur_date = datetime.today()
    records_end_year = cur_date.year if cur_date.month >= 4 else cur_date.year - 1
    source_urls = {
        str(x): base_url.format(year=x)
        for x in range(records_start_year, records_end_year + 1)
    }
    fetch_mapped_hosted_csvs(
        table_name,
        source_urls,
        transform_dataframe,
        ts_nodash=kwargs['ts_nodash'],
    )
