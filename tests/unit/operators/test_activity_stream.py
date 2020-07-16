from unittest import mock

import pytest
from requests.exceptions import HTTPError

from dataflow.config import ACTIVITY_STREAM_HAWK_CREDENTIALS
from dataflow.operators import activity_stream, api


def test_activity_stream_request(requests_mock):
    requests_mock.get(
        'http://test',
        request_headers={'Content-Type': 'application/json', 'Authorization': mock.ANY},
        json={'hits': []},
    )

    api._hawk_api_request('http://test', "GET", {}, ACTIVITY_STREAM_HAWK_CREDENTIALS)


def test_activity_stream_request_raises_error_without_hits(requests_mock):
    requests_mock.get('http://test', json={})

    with pytest.raises(ValueError):
        api._hawk_api_request(
            'http://test', "GET", {}, ACTIVITY_STREAM_HAWK_CREDENTIALS, 'hits'
        )


def test_activity_stream_request_raises_for_non_2xx_status(mocker, requests_mock):
    mocker.patch("time.sleep")  # skip backoff retry delay
    requests_mock.get('http://test', status_code=404)

    with pytest.raises(HTTPError):
        api._hawk_api_request(
            'http://test', "GET", {}, ACTIVITY_STREAM_HAWK_CREDENTIALS
        )


def test_fetch_from_activity_stream(mocker):
    mocker.patch.object(
        activity_stream.config, "ACTIVITY_STREAM_BASE_URL", "http://test"
    )
    req = mocker.patch.object(
        activity_stream,
        '_hawk_api_request',
        side_effect=[
            {
                "_shards": {"failures": [], "failed": 1},
                "hits": {"hits": [{"_source": "data", "sort": [120]}], "total": 100},
            },
            {
                "_shards": {},
                "hits": {"hits": [{"_source": "data2", "sort": [240]}], "total": 100},
            },
            {"_shards": {}, "hits": {"hits": [], "total": 100}},
        ],
        autospec=True,
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(activity_stream, "S3Data", return_value=s3_mock, autospec=True)

    activity_stream.fetch_from_activity_stream('table', 'index', {}, ts_nodash='task-1')
    req.assert_has_calls(
        [
            mock.call(
                'http://test/v3/index/_search',
                'GET',
                {'query': {}, 'size': 100, 'sort': [{'id': 'asc'}]},
                ACTIVITY_STREAM_HAWK_CREDENTIALS,
                'hits',
            ),
            mock.call(
                'http://test/v3/index/_search',
                'GET',
                {
                    'query': {},
                    'size': 100,
                    'sort': [{'id': 'asc'}],
                    'search_after': [120],
                },
                ACTIVITY_STREAM_HAWK_CREDENTIALS,
                'hits',
            ),
            mock.call(
                'http://test/v3/index/_search',
                'GET',
                {
                    'query': {},
                    'size': 100,
                    'sort': [{'id': 'asc'}],
                    'search_after': [240],
                },
                ACTIVITY_STREAM_HAWK_CREDENTIALS,
                'hits',
            ),
        ]
    )

    s3_mock.write_key.assert_has_calls(
        [
            mock.call('0000000001.json', ['data']),
            mock.call('0000000002.json', ['data2']),
        ]
    )
