import io
from datetime import datetime
from unittest import mock

import jwt
import pytest
from freezegun import freeze_time
from mohawk.exc import HawkFail
from requests import HTTPError

from dataflow.operators import common


FAKE_HAWK_CREDENTIALS = {"id": "some-id", "key": "some-key", "algorithm": "sha256"}


@pytest.fixture
def mock_sender(mocker):
    mock_sender = mocker.patch('dataflow.operators.common.Sender')
    mock_sender().request_header = 'dummy'


def test_hawk_api_request_fail(mocker, requests_mock):
    mock_sender = mocker.patch('dataflow.operators.common.Sender')
    mock_sender().request_header = 'dummy'
    mock_sender().accept_response.side_effect = HawkFail
    requests_mock.get(
        'http://test',
        headers={'Server-Authorization': 'dummy', 'Content-Type': ''},
        json={'next': None, 'results': []},
    )
    with pytest.raises(HawkFail):
        common._hawk_api_request(
            'http://test',
            credentials=FAKE_HAWK_CREDENTIALS,
            results_key="results",
            next_key="next",
        )


def test_hawk_api_request(mock_sender, requests_mock):
    requests_mock.get(
        'http://test',
        headers={'Server-Authorization': 'dummy', 'Content-Type': ''},
        json={'next': None, 'results': []},
    )
    common._hawk_api_request(
        'http://test',
        credentials=FAKE_HAWK_CREDENTIALS,
        results_key="results",
        next_key="next",
    )


def test_fetch_raises_error_invalid_response(mock_sender, requests_mock):
    requests_mock.get(
        'http://test',
        headers={'Server-Authorization': 'dummy', 'Content-Type': ''},
        json={'next': None},
    )

    with pytest.raises(ValueError):
        common._hawk_api_request(
            'http://test',
            credentials=FAKE_HAWK_CREDENTIALS,
            results_key="results",
            next_key="next",
        )


def test_fetch_raises_for_non_2xx_status(mocker, mock_sender, requests_mock):
    mocker.patch("time.sleep")  # skip backoff retry delay
    requests_mock.get(
        'http://test',
        headers={'Server-Authorization': 'dummy', 'Content-Type': ''},
        status_code=404,
    )
    with pytest.raises(HTTPError):
        common._hawk_api_request(
            'http://test',
            credentials=FAKE_HAWK_CREDENTIALS,
            results_key="results",
            next_key="next",
        )


def test_fetch(mocker):
    req = mocker.patch.object(
        common,
        '_hawk_api_request',
        side_effect=[
            {
                'next': 'http://test',
                'results': [{'id': 1, 'name': 'record1'}, {'id': 2, 'name': 'record2'}],
            },
            {'next': None, 'results': [{'id': 3, 'name': 'record3'}]},
        ],
        autospec=True,
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(common, "S3Data", return_value=s3_mock, autospec=True)

    common.fetch_from_hawk_api(
        'table',
        'http://test',
        hawk_credentials=FAKE_HAWK_CREDENTIALS,
        ts_nodash='task-1',
    )
    req.assert_has_calls(
        [
            mock.call(
                'http://test',
                credentials=FAKE_HAWK_CREDENTIALS,
                next_key='next',
                results_key='results',
                validate_response=True,
                force_http=False,
            )
        ]
    )

    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                [{'id': 1, 'name': 'record1'}, {'id': 2, 'name': 'record2'}],
            )
        ]
    )


def test_token_auth_invalid_response(mocker, requests_mock):
    s3_mock = mock.MagicMock()
    mocker.patch.object(common, "S3Data", return_value=s3_mock, autospec=True)
    requests_mock.get(
        'http://test',
        headers={'Authorization': 'token test-token'},
        json={'next': None},
    )

    with pytest.raises(ValueError):
        common.fetch_from_api_endpoint(
            'test_table',
            'http://test',
            'test-token',
            results_key="results",
            next_key="next",
            ts_nodash='token-auth-test',
        )


def test_token_auth_request_fail(mocker, mock_sender, requests_mock):
    mocker.patch("time.sleep")  # skip backoff retry delay
    requests_mock.get('http://test', headers={'token': 'test-token'}, status_code=404)
    with pytest.raises(HTTPError):
        common.fetch_from_api_endpoint(
            'test_table', 'http://test', 'test-token', ts_nodash='token-auth-test'
        )


def test_token_auth_request(mocker, requests_mock):
    s3_mock = mock.MagicMock()
    mocker.patch.object(common, "S3Data", return_value=s3_mock, autospec=True)
    requests_mock.get(
        'http://test',
        [
            {
                'status_code': 200,
                'json': {
                    'next': 'http://test',
                    'results': [
                        {'id': 1, 'name': 'record1'},
                        {'id': 2, 'name': 'record2'},
                    ],
                },
            },
            {
                'status_code': 200,
                'json': {'next': None, 'results': [{'id': 3, 'name': 'record3'}]},
            },
        ],
        request_headers={"Authorization": "Token test-token"},
    )
    common.fetch_from_api_endpoint(
        'test_table', 'http://test', auth_token='test-token', ts_nodash='task-1'
    )
    assert requests_mock.call_count == 2
    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                [{'id': 1, 'name': 'record1'}, {'id': 2, 'name': 'record2'}],
            ),
            mock.call('0000000002.json', [{'id': 3, 'name': 'record3'}]),
        ]
    )


def test_token_auth_builder_request(mocker, requests_mock):
    s3_mock = mock.MagicMock()
    mocker.patch.object(common, "S3Data", return_value=s3_mock, autospec=True)
    requests_mock.get(
        'http://test',
        [
            {
                'status_code': 200,
                'json': {
                    'next': 'http://test',
                    'results': [
                        {'id': 1, 'name': 'record1'},
                        {'id': 2, 'name': 'record2'},
                    ],
                },
            },
            {
                'status_code': 200,
                'json': {'next': None, 'results': [{'id': 3, 'name': 'record3'}]},
            },
        ],
        request_headers={"Authorization": "Token test-token-2020-01-01T12:00:00"},
    )

    with freeze_time('2020-01-01T12:00:00.000000'):
        common.fetch_from_api_endpoint(
            'test_table',
            'http://test',
            auth_token_builder=lambda: 'test-token-' + datetime.now().isoformat(),
            ts_nodash='task-1',
        )

    assert requests_mock.call_count == 2
    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                [{'id': 1, 'name': 'record1'}, {'id': 2, 'name': 'record2'}],
            ),
            mock.call('0000000002.json', [{'id': 3, 'name': 'record3'}]),
        ]
    )


def test_fetch_from_jwt_api_with_static_payload(mocker):
    token_api_fetch_method = mocker.patch.object(
        common, 'fetch_from_api_endpoint', autospec=True,
    )
    mocker.patch.object(jwt, "encode", return_value=b"jwt-token")

    common.fetch_from_jwt_api(
        'test_table', 'http://test', "s3cr3t", "FAKE123", payload={"foo": "bar"}
    )
    token_api_fetch_method.assert_called_once_with(
        'test_table',
        'http://test',
        results_key='results',
        next_key='next',
        auth_type='Bearer',
        auth_token='jwt-token',
    )


def test_fetch_from_jwt_api_with_payload_builder(mocker):
    token_api_fetch_method = mocker.patch.object(
        common, 'fetch_from_api_endpoint', autospec=True,
    )
    mocker.patch.object(jwt, "encode", return_value=b"jwt-token")

    def payload_builder():
        return {"foo": "bar"}

    common.fetch_from_jwt_api(
        'test_table',
        'http://test',
        "s3cr3t",
        "FAKE123",
        payload_builder=payload_builder,
    )
    token_api_fetch_method.assert_called_once_with(
        'test_table',
        'http://test',
        results_key='results',
        next_key='next',
        auth_type='Bearer',
        auth_token_builder=mocker.ANY,
    )


@pytest.mark.parametrize(
    'allow_empty_strings,expected_output',
    [
        (
            True,
            [
                {'col1': 'a', 'col2': '1', 'col3': ''},
                {'col1': 'b', 'col2': '2', 'col3': 'test'},
            ],
        ),
        (
            False,
            [
                {'col1': 'a', 'col2': '1', 'col3': None},
                {'col1': 'b', 'col2': '2', 'col3': 'test'},
            ],
        ),
    ],
)
def test_hosted_csv_request(
    mocker, requests_mock, allow_empty_strings, expected_output
):
    s3_mock = mock.MagicMock()
    mocker.patch.object(
        common, "S3Data", return_value=s3_mock, autospec=True,
    )
    csvfile = io.BytesIO(b'col1,col2,col3\n"a",1,\n"b",2,"test"\n"c",3,""')
    requests_mock.get('http://test', body=csvfile)
    common.fetch_from_hosted_csv(
        'test_table',
        'http://test',
        page_size=2,
        allow_empty_strings=allow_empty_strings,
        ts_nodash='task-1',
    )
    s3_mock.write_key.assert_has_calls([mock.call('0000000001.json', expected_output,)])
