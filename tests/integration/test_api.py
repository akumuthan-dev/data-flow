import os

import mohawk
import pytest
from airflow.www.app import create_app

from dataflow import api_auth_backend


@pytest.mark.parametrize(
    'auth_backend, expected_status',
    (
        ('airflow.api.auth.backend.default', 200),
        ('airflow.api.auth.backend.deny_all', 403),
        ('dataflow.api_auth_backend', 401),
    ),
)
def test_no_hawk_auth(auth_backend, expected_status):
    os.environ['AIRFLOW__API__AUTH_BACKEND'] = auth_backend
    app = create_app(testing=True)
    with app.test_client() as client:
        response = client.get('/api/experimental/test')
        assert response.status_code == expected_status


@pytest.mark.parametrize(
    'key, expected_status',
    (
        ('validkey', 200),
        ('invalidkey', 401),
    ),
)
def test_hawk_auth(key, expected_status, mocker):
    os.environ['AIRFLOW__API__AUTH_BACKEND'] = 'dataflow.api_auth_backend'
    url = 'http://0.0.0.0:8000/api/experimental/test'
    mocker.patch.object(
        api_auth_backend.config,
        'AIRFLOW_API_HAWK_CREDENTIALS',
        {'hawkid': 'validkey'},
    )
    app = create_app(testing=True)
    with app.test_client() as client:
        sender = mohawk.Sender(
            credentials={'id': 'hawkid', 'key': key, 'algorithm': 'sha256'},
            url=url,
            method='GET',
            content='',
            content_type='',
        )
        response = client.get(
            url, headers={'Authorization': sender.request_header, 'Content-Type': ''}
        )
        assert response.status_code == expected_status
