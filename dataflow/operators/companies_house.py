import codecs
import csv
import io
import zipfile

import backoff
import requests

from dataflow.utils import S3Data, logger


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def _download(source_url):
    response = requests.get(source_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error(f"Request failed: {response.text}")
        raise

    return response.content


def fetch_companies_house_companies(
    table_name: str,
    source_url: str,
    number_of_files: int,
    page_size: int = 10000,
    **kwargs,
):
    """
    Loop through `number_of_files`, build the url, download the zip file,
    extract and write data in batches of `page_size` to s3
    """
    s3 = S3Data(table_name, kwargs['ts_nodash'])
    page = 1
    results = []
    for file_num in range(1, number_of_files + 1):
        url = source_url.format(
            file_date=kwargs['next_execution_date'].strftime('%Y-%m-01'),
            file_num=file_num,
            num_files=number_of_files,
        )
        logger.info(f'Fetching zip file from {url}')
        with zipfile.ZipFile(io.BytesIO(_download(url))) as archive:
            with archive.open(archive.namelist()[0], 'r') as f:
                reader = csv.DictReader(codecs.iterdecode(f.readlines(), 'utf-8'))
                if reader.fieldnames is not None:
                    reader.fieldnames = [x.strip() for x in reader.fieldnames]
                for row in reader:
                    results.append(row)
                    if len(results) >= page_size:
                        s3.write_key(f'{page:010}.json', results)
                        results = []
                        page += 1

    if results:
        s3.write_key(f'{page:010}.json', results)

    logger.info('Fetching from source completed')
