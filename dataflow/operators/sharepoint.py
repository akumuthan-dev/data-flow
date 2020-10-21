import requests
from msal import ConfidentialClientApplication

from dataflow.config import DIT_SHAREPOINT_CREDENTIALS
from dataflow.utils import S3Data, logger


class InvalidAuthCredentialsError(ValueError):
    pass


def _make_request(url, access_token, params):
    response = requests.get(
        url,
        params=params,
        headers={'Authorization': f'Bearer {access_token}'},
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error('Request failed: %s', response.text)
        raise
    return response.json()


def fetch_from_sharepoint_list(
    table_name: str, sub_site_id: str, list_id: str, **kwargs
):
    s3 = S3Data(table_name, kwargs["ts_nodash"])
    app = ConfidentialClientApplication(
        DIT_SHAREPOINT_CREDENTIALS['client_id'],
        authority=f'https://login.microsoftonline.com/{DIT_SHAREPOINT_CREDENTIALS["tenant_id"]}',
        client_credential=DIT_SHAREPOINT_CREDENTIALS['client_secret'],
    )
    token_response = app.acquire_token_for_client(
        scopes=['https://graph.microsoft.com/.default']
    )
    if 'access_token' not in token_response:
        raise InvalidAuthCredentialsError(
            f'Failed to acquire token: {token_response.get("error_description")}'
        )

    access_token = token_response['access_token']
    tenant = DIT_SHAREPOINT_CREDENTIALS["tenant_domain"]
    sharepoint_site = (
        f':/sites/{DIT_SHAREPOINT_CREDENTIALS["site_name"]}:/sites/{sub_site_id}'
    )
    list_url = f'https://graph.microsoft.com/v1.0/sites/{tenant}{sharepoint_site}/lists/{list_id}'
    items_url = f'{list_url}/items'

    # Fetch a copy of the column names, this needs to be done separately from
    # fetching items as otherwise paging will not work.
    logger.info('Fetching column names from %s', list_url)
    graph_data = _make_request(list_url, access_token, {'expand': 'columns'})
    column_map = {col['name']: col['displayName'] for col in graph_data['columns']}

    # Fetch the list item data
    page = 1
    while items_url is not None:
        logger.info('Fetching from %s', items_url)
        graph_data = _make_request(items_url, access_token, {'expand': 'fields'})
        records = [
            {
                **{
                    column_map[col_id]: row['fields'][col_id]
                    for col_id in row['fields'].keys()
                    if col_id in column_map
                },
                'createdBy': row['createdBy']['user'],
                'lastModifiedBy': row['lastModifiedBy']['user']
                if row.get('lastModifiedBy') is not None
                else None,
            }
            for row in graph_data['value']
        ]
        s3.write_key(f"{page:010}.json", records)
        page += 1
        items_url = graph_data.get('@odata.nextLink')

    logger.info('Finished fetching from sharepoint')
