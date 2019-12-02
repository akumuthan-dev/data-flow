from unittest import mock

import pytest
from requests.exceptions import HTTPError

from dataflow.operators import activity_stream


def test_activity_stream_request(requests_mock):
    requests_mock.get(
        'http://test',
        request_headers={'Content-Type': 'application/json', 'Authorization': mock.ANY},
        json={'hits': []},
    )

    activity_stream._activity_stream_request('http://test', {})


def test_activity_stream_request_raises_error_without_hits(requests_mock):
    requests_mock.get('http://test', json={})

    with pytest.raises(ValueError):
        activity_stream._activity_stream_request('http://test', {})


def test_activity_stream_request_raises_for_non_2xx_status(requests_mock):
    requests_mock.get('http://test', status_code=404)

    with pytest.raises(HTTPError):
        activity_stream._activity_stream_request('http://test', {})


def test_fetch_from_activity_stream(mocker):
    mocker.patch.object(
        activity_stream.config, "ACTIVITY_STREAM_BASE_URL", "http://test"
    )
    req = mocker.patch.object(
        activity_stream,
        '_activity_stream_request',
        side_effect=[
            {
                "_shards": {"failures": [], "failed": 1},
                "hits": {"hits": [{"_source": "data", "sort": [120]}], "total": 100},
            },
            {
                "_shards": {},
                "hits": {"hits": [{"_source": "data", "sort": [240]}], "total": 100},
            },
            {"_shards": {}, "hits": {"hits": [], "total": 100}},
        ],
        autospec=True,
    )

    redis = mock.Mock()
    mocker.patch.object(
        activity_stream, 'get_redis_client', return_value=redis, autospec=True
    )

    var_set = mocker.patch.object(activity_stream.Variable, 'set', autospec=True)

    activity_stream.fetch_from_activity_stream('index', {}, 'task-1')
    req.assert_has_calls(
        [
            mock.call(
                'http://test/v3/index/_search',
                {'query': {}, 'size': 100, 'sort': [{'id': 'asc'}]},
            ),
            mock.call(
                'http://test/v3/index/_search',
                {
                    'query': {},
                    'size': 100,
                    'sort': [{'id': 'asc'}],
                    'search_after': [120],
                },
            ),
            mock.call(
                'http://test/v3/index/_search',
                {
                    'query': {},
                    'size': 100,
                    'sort': [{'id': 'asc'}],
                    'search_after': [240],
                },
            ),
        ]
    )

    redis.rpush.assert_has_calls(
        [mock.call('task-1', 'task-1_1'), mock.call('task-1', 'task-1_2')]
    )

    var_set.assert_has_calls(
        [
            mock.call('task-1_1', ['data'], serialize_json=True),
            mock.call('task-1_2', ['data'], serialize_json=True),
        ]
    )
