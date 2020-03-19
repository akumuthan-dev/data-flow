from unittest import mock

import pytest
from mohawk.exc import HawkFail
from requests import HTTPError

from dataflow.operators import common


FAKE_HAWK_CREDENTIALS = {
    "id": "some-id",
    "key": "some-key",
    "algorithm": "sha256",
}


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
