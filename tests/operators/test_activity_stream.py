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
        activity_stream.config, "ACTIVITY_STREAM_BASE_URL", "http://test/"
    )
    req = mocker.patch.object(
        activity_stream,
        '_activity_stream_request',
        side_effect=[
            {'hits': {'hits': [{"_source": "data"}]}},
            {'hits': {'hits': [], 'total': 100}},
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
            mock.call('http://test/index', {'size': 100, 'from': 0}),
            mock.call('http://test/index', {'size': 100, 'from': 100}),
        ]
    )

    redis.rpush.assert_has_calls(
        [mock.call('task-1', 'task-10'), mock.call('task-1', 'task-1100')]
    )

    var_set.assert_has_calls(
        [
            mock.call('task-10', ['data'], serialize_json=True),
            mock.call('task-1100', [], serialize_json=True),
        ]
    )
