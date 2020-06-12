import io
from typing import Callable, Dict

import pandas as pd
import requests

from dataflow.utils import S3Data, logger


def fetch_mapped_hosted_csvs(
    table_name: str,
    source_urls: Dict[str, str],
    df_transform: Callable[[pd.DataFrame], pd.DataFrame],
    page_size: int = 10000,
    **kwargs,
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])

    page = 1
    for type_, source_url in source_urls.items():
        logger.info(f"Fetching {source_url}")
        response = requests.get(source_url)
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        df = df_transform(df)
        df["source_url_key"] = type_

        for i in range((len(df) // page_size) + 1):
            results = df.iloc[page_size * i : page_size * (i + 1)].to_json(
                orient="records", date_format="iso"
            )
            s3.write_key(f"{page:010}.json", results, jsonify=False)
            page += 1

    logger.info("Fetching from source completed")
