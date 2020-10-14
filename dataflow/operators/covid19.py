import io
from typing import Callable

import requests
import pandas as pd

from dataflow.utils import S3Data, logger


def fetch_apple_mobility_data(
    table_name: str,
    base_url: str,
    config_path: str,
    df_transform: Callable[[pd.DataFrame], pd.DataFrame],
    page_size: int = 1000,
    **kwargs,
):
    s3 = S3Data(table_name, kwargs['ts_nodash'])

    api_config = requests.get(base_url + config_path).json()
    source_url = (
        base_url + api_config['basePath'] + api_config['regions']['en-us']['csvPath']
    )
    logger.info(f'Fetching csv from {source_url}')
    response = requests.get(source_url)
    df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
    df = df_transform(df)

    page = 1
    for i in range((len(df) // page_size) + 1):
        results = df.iloc[page_size * i : page_size * (i + 1)].to_json(
            orient="records", date_format="iso"
        )
        s3.write_key(f"{page:010}.json", results, jsonify=False)
        page += 1

    logger.info('Fetching from source completed')
