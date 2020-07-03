from unittest import mock

import pytest
from requests import HTTPError

from dataflow.operators import sharepoint


@pytest.fixture
def mock_msal_app(mocker):
    msal_app = mock.MagicMock()
    msal_app.acquire_token_for_client.return_value = {'access_token': 'a-valid-token'}
    mocker.patch(
        'dataflow.operators.sharepoint.ConfidentialClientApplication',
        return_value=msal_app,
        autospec=True,
    )
    return msal_app


def test_sharepoint_request_auth_fail(mock_msal_app):
    mock_msal_app.acquire_token_for_client.return_value = {
        'error_description': 'Auth failed'
    }
    with pytest.raises(sharepoint.InvalidAuthCredentialsError):
        sharepoint.fetch_from_sharepoint_list(
            'test_table', 'test-site-2', 'test-list', ts_nodash='test',
        )


def test_sharepoint_request_fail(mock_msal_app, requests_mock):
    sharepoint.DIT_SHAREPOINT_CREDENTIALS['tenant_domain'] = 'tenant.sharepoint.com'
    sharepoint.DIT_SHAREPOINT_CREDENTIALS['site_name'] = 'test-site-1'
    requests_mock.get(
        'https://graph.microsoft.com/v1.0/sites/tenant.sharepoint.com:/sites/test-site-1:/sites/test-site-2/lists/test-list',
        status_code=403,
    )
    with pytest.raises(HTTPError):
        sharepoint.fetch_from_sharepoint_list(
            'test_table', 'test-site-2', 'test-list', ts_nodash='test',
        )


def test_sharepoint_request(mock_msal_app, mocker, requests_mock):
    s3_mock = mock.MagicMock()
    mocker.patch.object(
        sharepoint, "S3Data", return_value=s3_mock, autospec=True,
    )
    sharepoint.DIT_SHAREPOINT_CREDENTIALS['tenant_domain'] = 'tenant.sharepoint.com'
    sharepoint.DIT_SHAREPOINT_CREDENTIALS['site_name'] = 'test-site-1'
    requests_mock.get(
        'https://graph.microsoft.com/v1.0/sites/tenant.sharepoint.com:/sites/test-site-1:/sites/test-site-2/lists/test-list',
        status_code=200,
        json={
            'columns': [
                {'name': 'col1', 'displayName': 'ID'},
                {'name': 'col2', 'displayName': 'Name'},
            ],
            'items': [
                {
                    'fields': {'col1': 1, 'col2': 'record1'},
                    'createdBy': {'user': 'user1@test.com'},
                    'lastModifiedBy': {'user': 'user2@test.com'},
                },
                {
                    'fields': {'col1': 2, 'col2': 'record2'},
                    'createdBy': {'user': 'user2@test.com'},
                    'lastModifiedBy': {'user': 'user1@test.com'},
                },
            ],
        },
    )
    sharepoint.fetch_from_sharepoint_list(
        'test_table', 'test-site-2', 'test-list', ts_nodash='test',
    )
    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                [
                    {
                        'ID': 1,
                        'Name': 'record1',
                        'createdBy': 'user1@test.com',
                        'lastModifiedBy': 'user2@test.com',
                    },
                    {
                        'ID': 2,
                        'Name': 'record2',
                        'createdBy': 'user2@test.com',
                        'lastModifiedBy': 'user1@test.com',
                    },
                ],
            )
        ]
    )
