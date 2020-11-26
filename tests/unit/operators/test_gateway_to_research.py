from unittest import mock

from dataflow.operators import gateway_to_research


def test_token_auth_request(mocker, requests_mock):
    s3_mock = mock.MagicMock()
    mocker.patch.object(
        gateway_to_research, "S3Data", return_value=s3_mock, autospec=True
    )
    requests_mock.get(
        'https://gtr.ukri.org/gtr/api/funds',
        [
            {
                'status_code': 200,
                'json': {
                    "links": None,
                    "ext": None,
                    "page": 1,
                    "size": 100,
                    "totalPages": 2,
                    "totalSize": 200,
                    "fund": [{"id": "00000000-0000-0000-0000-000000000001"}],
                },
            },
            {
                'status_code': 200,
                'json': {
                    "links": None,
                    "ext": None,
                    "page": 2,
                    "size": 100,
                    "totalPages": 2,
                    "totalSize": 200,
                    "fund": [{"id": "00000000-0000-0000-0000-000000000002"}],
                },
            },
        ],
    )
    gateway_to_research.fetch_from_gtr_api('test_table', 'fund', ts_nodash="task-1")

    assert requests_mock.call_count == 2
    assert requests_mock.request_history[0].query == 'p=1&s=100'
    assert requests_mock.request_history[1].query == 'p=2&s=100'

    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json', [{"id": "00000000-0000-0000-0000-000000000001"}]
            ),
            mock.call(
                '0000000002.json', [{"id": "00000000-0000-0000-0000-000000000002"}]
            ),
        ]
    )
