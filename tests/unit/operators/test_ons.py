from unittest import mock

import pytest
from requests.exceptions import HTTPError

from dataflow.operators import ons


def test_ons_sparql_request(requests_mock):
    requests_mock.post(
        'http://test/',
        request_headers={'Accept': 'application/json'},
        json={'results': []},
    )

    ons._ons_sparql_request('http://test', "SELECT *", page=2, per_page=20)


def test_activity_stream_request_raises_error_without_hits(requests_mock):
    requests_mock.post('http://test', json={})

    with pytest.raises(ValueError):
        ons._ons_sparql_request('http://test', "SELECT *")


def test_activity_stream_request_raises_for_non_2xx_status(mocker, requests_mock):
    mocker.patch("time.sleep")  # skip backoff retry delay
    requests_mock.post('http://test', status_code=404)

    with pytest.raises(HTTPError):
        ons._ons_sparql_request('http://test', "SELECT *")


def test_fetch_from_activity_stream(mocker):
    req = mocker.patch.object(
        ons,
        '_ons_sparql_request',
        side_effect=[
            {"results": {"bindings": ["data"]}},
            {"results": {"bindings": ["data2"]}},
            {"results": {"bindings": []}},
        ],
        autospec=True,
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(ons, "S3Data", return_value=s3_mock, autospec=True)

    ons.fetch_from_ons_sparql('table', "SELECT *", None, ts_nodash='task-1')
    req.assert_has_calls(
        [
            mock.call(ons.config.ONS_SPARQL_URL, "SELECT *", page=1),
            mock.call(ons.config.ONS_SPARQL_URL, "SELECT *", page=2),
            mock.call(ons.config.ONS_SPARQL_URL, "SELECT *", page=3),
        ]
    )

    s3_mock.write_key.assert_has_calls(
        [
            mock.call('0000000001.json', ['data']),
            mock.call('0000000002.json', ['data2']),
        ]
    )


def test_fetch_from_activity_stream_with_index_query(mocker):
    req = mocker.patch.object(
        ons,
        '_ons_sparql_request',
        side_effect=[
            {"results": {"bindings": [{"label": {"value": "index_data"}}]}},
            {"results": {"bindings": ["data"]}},
            {"results": {"bindings": []}},
        ],
        autospec=True,
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(ons, "S3Data", return_value=s3_mock, autospec=True)

    ons.fetch_from_ons_sparql(
        'table', "SELECT {label[value]}", "SELECT *", ts_nodash='task-1'
    )
    req.assert_has_calls(
        [
            mock.call(ons.config.ONS_SPARQL_URL, "SELECT *"),
            mock.call(ons.config.ONS_SPARQL_URL, "SELECT index_data", page=1),
            mock.call(ons.config.ONS_SPARQL_URL, "SELECT index_data", page=2),
        ]
    )

    s3_mock.write_key.assert_has_calls(
        [mock.call('index_data-0000000001.json', ['data'])]
    )
