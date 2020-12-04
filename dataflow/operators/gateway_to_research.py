import requests

from dataflow.utils import S3Data, logger


def fetch_from_gtr_api(table_name: str, resource_type: str, **kwargs):
    source_url = 'https://gtr.ukri.org/gtr/api'

    s3 = S3Data(table_name, kwargs["ts_nodash"])
    page = 1

    while True:
        response = requests.get(
            f'{source_url}/{resource_type}s',
            params={'p': page, 's': 100},
            headers={'Accept': 'application/json'},
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.error(f"Request failed: {response.text}")
            raise

        response_json = response.json()
        total_pages = response_json['totalPages']
        total_number_of_results = response_json['totalSize']

        results = response_json[resource_type]

        s3.write_key(f"{page:010}.json", results)

        logger.info(
            f"Fetched {len(results*page)} out of {total_number_of_results} {resource_type} records"
        )

        page += 1
        if page > total_pages:
            break

    logger.info("Fetching from source completed")
