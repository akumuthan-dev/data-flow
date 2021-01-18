import mohawk
import pytest
import requests


@pytest.mark.parametrize(
    'key, expected_status', (('some-key', 200), ('invalidkey', 401),),
)
def test_hawk_auth(key, expected_status, mocker):
    url = 'http://localhost:8080/api/experimental/test'
    sender = mohawk.Sender(
        credentials={'id': 'dataflowapi', 'key': key, 'algorithm': 'sha256'},
        url=url,
        method='GET',
        content='',
        content_type='',
    )
    response = requests.get(
        url, headers={'Authorization': sender.request_header, 'Content-Type': ''}
    )
    assert response.status_code == expected_status
