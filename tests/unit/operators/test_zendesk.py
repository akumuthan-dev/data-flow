from datetime import datetime
from unittest import mock

import pytest
import requests

from dataflow.operators import zendesk

ZENDESK_CREDENTIALS = {
    "test": {"url": "local.host", "email": "EMAIL", "secret": "SECRET"}
}


def test_query_fail(mocker, requests_mock):
    url = "http://local.host/404"
    mocker.patch("time.sleep")
    mocker.patch.object(zendesk.config, "ZENDESK_CREDENTIALS", ZENDESK_CREDENTIALS)
    requests_mock.get(url, status_code=404)

    with pytest.raises(requests.exceptions.HTTPError):
        zendesk._query(url, "test")


def test_zendesk_fetch_daily_tickets(mocker):
    fn = "file-name"
    s3_data = mock.Mock(bucket="bucket", prefix="prefix-data")
    s3_upstream = mock.Mock(
        bucket="bucket",
        prefix="prefix-upstream",
        list_keys=lambda: [f"{s3_upstream.prefix}/{fn}"],
    )

    query = mock.Mock(on_exception=mock.Mock())
    data = [{"results": [{"a": 1}], "count": 1, "next_page": None}]

    mocker.patch.object(zendesk.config, "ZENDESK_CREDENTIALS", ZENDESK_CREDENTIALS)
    mocker.patch.object(zendesk, "S3Data", return_value=s3_data, autospec=True)
    mocker.patch.object(zendesk, "S3Upstream", return_value=s3_upstream, autospec=True)
    mocker.patch.object(
        zendesk, "_query", return_value=query, side_effect=data, autospec=True
    )

    zendesk.fetch_daily_tickets(
        schema_name="schema",
        table_name="test-table",
        account="test",
        execution_date=datetime(2000, 12, 23, 0, 11, 22),
        ts_nodash="20001223T001122",
    )
    assert s3_upstream.write_key.call_args == mock.call(
        "20001222.json", [{"a": 1}], jsonify=True
    )
    assert s3_data.client.copy_object.call_args == mock.call(
        source_bucket_key=f"{s3_upstream.prefix}/{fn}",
        dest_bucket_key=f"{s3_data.prefix}/{fn}",
        source_bucket_name="bucket",
        dest_bucket_name="bucket",
    )
