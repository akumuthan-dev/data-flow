import io
import zipfile
from unittest import mock

import pytest
import requests

from dataflow.operators import hmrc


@pytest.mark.parametrize(
    "value, expected",
    [("123", "123"), ("ABC", ValueError), ("A1", ValueError), ("", ValueError)],
)
def test_digits(value, expected):
    try:
        assert hmrc.digits(value) == expected
    except AssertionError:
        raise
    except expected:
        pass


@pytest.mark.parametrize(
    "value, expected",
    [
        ("+123", 123),
        ("-123", -123),
        ("123", ValueError),
        ("ABC", ValueError),
        ("", ValueError),
        ("+", ValueError),
        ("-", ValueError),
        ("+-123", ValueError),
        ("-+123", ValueError),
        ("+ABC", ValueError),
        ("-ABC", ValueError),
    ],
)
def test_leading_sign_digits(value, expected):
    try:
        assert hmrc.leading_sign_digits(value) == expected
    except AssertionError:
        raise
    except expected:
        pass


@pytest.mark.parametrize(
    "value, expected",
    [
        ("12/3456", "12/3456"),
        ("99/9999", ValueError),
        ("00/0000", ValueError),
        ("11/1111", "11/1111"),
        ("AA/AAAA", ValueError),
        ("12/AAAA", ValueError),
        ("", ValueError),
        ("1234567", ValueError),
        ("AAAAAAA", ValueError),
        ("12-3456", ValueError),
    ],
)
def test_mm_yyyy(value, expected):
    try:
        assert hmrc.mm_yyyy(value) == expected
    except AssertionError:
        raise
    except expected:
        pass


@pytest.fixture
def dummy_data():
    return (
        "000000000|00000|000|HMCUSTOMS MONTHLY DATA| NOVEMBER|2019|NON-EU EXPORTS    \n"
        "010121000|00150|000|028|NO|11/2019|204|IMM|008|DK|0|000|010|30|000|   |000|001|+000000000017640|+0000000000500|+0000000000001|000000000000000\n"
        "010121000|00150|000|028|NO|11/2019|495|ZLC|006|GB|0|000|030|00|000|   |000|001|+000000000001795|+0000000000500|+0000000000001|000000000000000\n"
        "010121000|00150|000|028|NO|11/2019|495|ZLC|006|GB|0|000|030|30|000|   |000|001|+000000000045000|+0000000000500|+0000000000001|000000000000000\n"
        "999999999|0000000195241|0000000000000|0000000000060|0000000001286|+00000016612067849|+00000000047645952\n"
    )


@pytest.mark.parametrize(
    "line_no, expected_value", [(1, 17640), (2, 1795), (3, 45000)],
)
def test_record_parser(dummy_data, line_no, expected_value):
    record = dummy_data.splitlines()[line_no].split("|")

    parsed_record = hmrc.ExportRecord(*record)

    assert parsed_record["value"] == expected_value


@pytest.mark.parametrize(
    "record, expected_error",
    [
        ("foo|bar", "Invalid record length"),
        ("|" + "0" * 140, "Wrong number of fields 2/22"),
        ("A" + "|" * 21 + "0" * 119, "Invalid length 1 for field comcode - expected 9"),
    ],
)
def test_invalid_record(record, expected_error):
    with pytest.raises(Exception) as e:
        hmrc.ExportRecord(*record.split("|"))
    assert expected_error in str(e.value)


@pytest.fixture
def dummy_export_records(dummy_data):
    return [
        hmrc.ExportRecord(*line.split("|")) for line in dummy_data.splitlines()[1:-1]
    ]


def test_export_record(dummy_export_records):
    schema_keys = set(key for key, _ in hmrc.ExportRecord.schema)
    record_keys = set(dummy_export_records[0].keys())

    schema_keys.remove("account_mmyyyy")
    schema_keys.add("account_mm")
    schema_keys.add("account_ccyy")

    assert schema_keys == record_keys


@pytest.fixture
def mock_data_download(dummy_data, requests_mock):
    in_memory = io.BytesIO()
    with zipfile.ZipFile(in_memory, "w") as zf:
        zf.writestr("dummy_data", dummy_data)

    requests_mock.get(
        "http://test/dummy_data.zip", content=in_memory.getvalue(),
    )


def test_download_datafile(dummy_data, mock_data_download):
    data = hmrc._download_datafile("http://test/dummy_data.zip")
    assert data == dummy_data.encode("utf-8")


def test_download_fail(mocker, requests_mock):
    mocker.patch("time.sleep")
    requests_mock.get("http://test/dummy_data.zip", status_code=404)

    with pytest.raises(requests.exceptions.HTTPError):
        hmrc._download_datafile("http://test/dummy_data.zip")


def test_fetch_from_uktradeinfo(mocker, mock_data_download, dummy_export_records):
    s3_mock = mock.MagicMock()
    mocker.patch.object(hmrc, "S3Data", return_value=s3_mock, autospec=True)
    mocker.patch.object(hmrc.config, "HMRC_UKTRADEINFO_URL", "http://test")

    hmrc.fetch_from_uktradeinfo(
        "non-eu-exports", "dummy_data.zip", ts_nodash="task-1",
    )

    s3_mock.write_key.assert_called_once_with(
        "0000000001.json", dummy_export_records,
    )
