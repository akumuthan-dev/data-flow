from unittest import mock

from dataflow.operators import csv_inputs


def test_fetch_mapped_hosted_csvs(mocker, requests_mock):
    s3_mock = mock.Mock()

    def transform(x):
        x["modified"] = True

        return x

    mocker.patch.object(csv_inputs, "S3Data", return_value=s3_mock, autospec=True)
    requests_mock.get("http://test1", text="1\n1\n1\n1\n")
    requests_mock.get("http://test2", text="2\n2\n2\n2\n")

    csv_inputs.fetch_mapped_hosted_csvs(
        table_name="test",
        source_urls={"one": "http://test1", "two": "http://test2"},
        df_transform=transform,
        page_size=2,
        ts_nodash="123",
    )

    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                '[{"1":1,"modified":true,"source_url_key":"one"},{"1":1,"modified":true,"source_url_key":"one"}]',
                jsonify=False,
            ),
            mock.call(
                '0000000002.json',
                '[{"1":1,"modified":true,"source_url_key":"one"}]',
                jsonify=False,
            ),
            mock.call(
                '0000000003.json',
                '[{"2":2,"modified":true,"source_url_key":"two"},{"2":2,"modified":true,"source_url_key":"two"}]',
                jsonify=False,
            ),
            mock.call(
                '0000000004.json',
                '[{"2":2,"modified":true,"source_url_key":"two"}]',
                jsonify=False,
            ),
        ]
    )
