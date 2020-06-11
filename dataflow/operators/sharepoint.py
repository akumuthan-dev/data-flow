import requests
from msal import ConfidentialClientApplication

from dataflow.config import DIT_SHAREPOINT_CREDENTIALS
from dataflow.utils import S3Data, logger


class InvalidAuthCredentialsError(ValueError):
    pass


def fetch_from_sharepoint_list(
    table_name: str, site_name: str, list_name: str, **kwargs
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

    response = requests.get(
        f'https://graph.microsoft.com/v1.0/sites/{DIT_SHAREPOINT_CREDENTIALS["tenant_domain"]}:/sites/{site_name}:/lists/{list_name}',
        params={'expand': 'columns,items(expand=fields)'},
        headers={'Authorization': f'Bearer {token_response["access_token"]}'},
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.error(f'Request failed: {response.text}')
        raise

    graph_data = response.json()
    column_map = {col['name']: col['displayName'] for col in graph_data['columns']}
    records = [
        {
            **{
                column_map[col_id]: row['fields'][col_id]
                for col_id in row['fields'].keys()
                if col_id in column_map
            },
            'createdBy': row['createdBy']['user'],
            'lastModifiedBy': row['lastModifiedBy']['user'],
        }
        for row in graph_data['items']
    ]
    s3.write_key(f"{1:010}.json", records)
