import datetime
import io
import zipfile
from unittest import mock

import pytest
import requests
import sqlalchemy

from dataflow.dags.hmrc_pipelines import HMRCNonEUExports
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
def dummy_export_data():
    return (
        "000000000|00000|000|HMCUSTOMS MONTHLY DATA| NOVEMBER|2019|NON-EU EXPORTS    \n"
        "010121000|00150|000|028|NO|11/2019|204|IMM|008|DK|0|000|010|30|000|   |000|001|+000000000017640|+0000000000500|+0000000000001|000000000000000\n"
        "010121000|00150|000|028|NO|11/2019|495|ZLC|006|GB|0|000|030|00|000|   |000|001|+000000000001795|+0000000000500|+0000000000001|000000000000000\n"
        "010121000|00150|000|028|NO|11/2019|495|ZLC|006|GB|0|000|030|30|000|   |000|001|+000000000045000|+0000000000500|+0000000000001|000000000000000\n"
        "999999999|0000000195241|0000000000000|0000000000060|0000000001286|+00000016612067849|+00000000047645952\n"
    )


@pytest.fixture
def dummy_import_data():
    return (
        "000000000|00000|000|HMCUSTOMS MONTHLY DATA| DECEMBER|2019|NON-EU IMPORTS    \n"
        "010121000|00150|000|028|NO|028|NO|12/2019|204|IMM|008|DK|028|NO|0|001|010|00|000|   |000|000|000|+000000000003235|+0000000000500|+0000000000001\n"
        "010121000|00150|000|039|CH|039|CH|12/2019|495|ZLC|006|GB|039|CH|0|000|030|00|000|   |000|000|000|+000000000004350|+0000000000600|+0000000000001\n"
        "010121000|00150|000|400|US|006|GB|12/2019|434|LSA|400|US|400|US|0|000|040|00|000|   |000|000|000|+000000000303575|+0000000000500|+0000000000001\n"
        "999999999|0000000186050|0000000000000|0000000000061|0000000000514|+00000019759338026|+00000000045540476\n"
    )


@pytest.mark.parametrize(
    "line_no, expected_value", [(1, 17640), (2, 1795), (3, 45000)],
)
def test_export_record_parser(dummy_export_data, line_no, expected_value):
    record = dummy_export_data.splitlines()[line_no].split("|")

    parsed_record = hmrc.ExportRecord(*record)

    assert parsed_record["value"] == expected_value


@pytest.mark.parametrize(
    "line_no, expected_value", [(1, 3235), (2, 4350), (3, 303575)],
)
def test_import_record_parser(dummy_import_data, line_no, expected_value):
    record = dummy_import_data.splitlines()[line_no].split("|")

    parsed_record = hmrc.ImportRecord(*record)

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


def test_export_record(dummy_export_data):
    export_records = [
        hmrc.ExportRecord(*line.split("|"))
        for line in dummy_export_data.splitlines()[1:-1]
    ]
    schema_keys = set(key for key, _ in hmrc.ExportRecord.schema)
    record_keys = set(export_records[0].keys())

    schema_keys.remove("account_mmyyyy")
    schema_keys.add("account_mm")
    schema_keys.add("account_ccyy")

    assert schema_keys == record_keys


DUMMY_DATA_FILENAME = "SMKE191912.zip"


def test_download_fail(mocker, requests_mock):
    mocker.patch("time.sleep")
    requests_mock.get(f"http://test/{DUMMY_DATA_FILENAME}", status_code=404)

    with pytest.raises(requests.exceptions.HTTPError):
        hmrc._download(f"http://test/{DUMMY_DATA_FILENAME}")


def test_fetch_from_uktradeinfo(mocker, requests_mock, dummy_export_data):
    in_memory = io.BytesIO()
    with zipfile.ZipFile(in_memory, "w") as zf:
        zf.writestr("dummy_export_data", dummy_export_data)
    requests_mock.get(
        f"http://test/{DUMMY_DATA_FILENAME}", content=in_memory.getvalue(),
    )
    s3_mock = mock.MagicMock()
    mocker.patch.object(hmrc, "S3Data", return_value=s3_mock, autospec=True)
    mocker.patch.object(hmrc.config, "HMRC_UKTRADEINFO_URL", "http://test")
    run_date = datetime.datetime(2019, 12, 1)

    hmrc.fetch_non_eu_data_from_uktradeinfo(
        "non-eu-exports", "SMKE19", ts_nodash="task-1", run_date=run_date,
    )

    s3_mock.write_key.assert_called_once_with(
        "SMKE191912", dummy_export_data.encode("utf-8"),
    )


@pytest.fixture
def table():
    return sqlalchemy.Table(
        "test_table",
        sqlalchemy.MetaData(),
        sqlalchemy.Column("id", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.Column("data", sqlalchemy.Integer()),
    )


@pytest.fixture
def s3(mocker):
    s3_mock = mock.MagicMock()
    mocker.patch.object(hmrc, "S3Data", return_value=s3_mock, autospec=True)
    return s3_mock


def test_insert_data_into_db(dummy_export_data, mocker, mock_db_conn, s3):
    table = mock.Mock()
    mocker.patch.object(hmrc, "_get_temp_table", autospec=True, return_value=table)

    s3.iter_keys.return_value = [
        ("SMKE191912", dummy_export_data.encode("utf-8")),
    ]

    hmrc.insert_data_into_db(
        "test-db",
        table,
        HMRCNonEUExports.field_mapping,
        hmrc.ExportRecord,
        ts_nodash="123",
    )

    export_records = [
        hmrc.ExportRecord(*line.split("|"))
        for line in dummy_export_data.splitlines()[1:-1]
    ]

    mock_db_conn.execute.assert_has_calls(
        [
            mock.call(table.insert(), **export_records[0]),
            mock.call(table.insert(), **export_records[1]),
            mock.call(table.insert(), **export_records[2]),
        ]
    )

    s3.iter_keys.assert_called_once_with()
