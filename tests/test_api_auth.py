from unittest import mock

import mohawk
from flask import Flask

from dataflow import api_auth_backend

TEST_CLIENT_ID = 'test-hawk-client-id'
TEST_CLIENT_KEY = 'test-hawk-client-key'


def test_auth_header():
    app = Flask('dataflow-test-app')
    mock_view = mock.Mock()
    with app.test_request_context():
        response = api_auth_backend.requires_authentication(mock_view)()
        assert response.status_code == 401
    mock_view.assert_not_called()


def test_invalid_hawk_id(mocker):
    app = Flask('dataflow-test-app')
    mock_view = mock.Mock()
    mocker.patch.object(
        api_auth_backend.config,
        'AIRFLOW_API_HAWK_CREDENTIALS',
        ((TEST_CLIENT_ID, TEST_CLIENT_KEY),),
    )
    credentials = {'id': 'invalid-id', 'key': TEST_CLIENT_KEY, 'algorithm': 'sha256'}
    sender = mohawk.Sender(
        credentials=credentials, url='/', method='GET', content='', content_type='',
    )
    headers = {'Authorization': sender.request_header, 'Content-Type': ''}
    with app.test_request_context('/', headers=headers):
        response = api_auth_backend.requires_authentication(mock_view)()
        assert response.status_code == 401
    mock_view.assert_not_called()


def test_invalid_hawk_key(mocker):
    app = Flask('dataflow-test-app')
    mock_view = mock.Mock()
    mocker.patch.object(
        api_auth_backend.config,
        'AIRFLOW_API_HAWK_CREDENTIALS',
        ((TEST_CLIENT_ID, TEST_CLIENT_KEY),),
    )
    credentials = {'id': TEST_CLIENT_ID, 'key': 'invalid-key', 'algorithm': 'sha256'}
    sender = mohawk.Sender(
        credentials=credentials, url='/', method='GET', content='', content_type='',
    )
    headers = {'Authorization': sender.request_header, 'Content-Type': ''}
    with app.test_request_context('/', headers=headers):
        response = api_auth_backend.requires_authentication(mock_view)()
        assert response.status_code == 401
    mock_view.assert_not_called()


def test_valid_credentials(mocker):
    app = Flask('dataflow-test-app')
    mocker.patch.object(
        api_auth_backend.config,
        'AIRFLOW_API_HAWK_CREDENTIALS',
        ((TEST_CLIENT_ID, TEST_CLIENT_KEY),),
    )
    credentials = {'id': TEST_CLIENT_ID, 'key': TEST_CLIENT_KEY, 'algorithm': 'sha256'}
    sender = mohawk.Sender(
        credentials=credentials,
        url='http://localhost/',
        method='GET',
        content='',
        content_type='',
    )
    mock_view = mock.Mock()
    mock_view.return_value = {}
    headers = {'Authorization': sender.request_header, 'Content-Type': ''}
    with app.test_request_context('/', headers=headers):
        response = api_auth_backend.requires_authentication(mock_view)()
        assert response.status_code == 200
    mock_view.assert_called_once()
