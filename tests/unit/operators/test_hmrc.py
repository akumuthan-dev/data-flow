import datetime
import io
import zipfile
from unittest import mock

import pytest
import pytz
import requests

from dataflow.operators import hmrc


DUMMY_DATA_FILENAME = "SMKE191912.zip"


def test_download_fail(mocker, requests_mock):
    mocker.patch("time.sleep")
    requests_mock.get(f"http://test/{DUMMY_DATA_FILENAME}", status_code=404)

    with pytest.raises(requests.exceptions.HTTPError):
        hmrc._download(f"http://test/{DUMMY_DATA_FILENAME}")


def test_fetch_from_uktradeinfo(mocker, requests_mock):
    html_archive = (
        '<a href="http://test/a/unrelated.zip"></a>'
        '<a href="http://test/b/smke19_2019archive.zip"></a>'
        '<a href="http://test/d/unrelated.zip"></a>'
    )

    html_latest = (
        '<a href="http://test/e/unrelated.zip"></a>'
        '<a href="http://test/f/smke192002.zip"></a>'
        '<a href="http://test/g/smke19_2020archive.zip"></a>'
        '<a href="http://test/h/unrelated.zip"></a>'
    )

    file1 = io.BytesIO()
    with zipfile.ZipFile(file1, "w") as zf:
        zf.writestr(
            "dummy_export_data",
            (
                "000000000|00000|000|HMCUSTOMS MONTHLY DATA| NOVEMBER|2019|NON-EU EXPORTS    \n"
                "010121000|00150|000|028|NO|11/2019|204|IMM|008|DK|0|000|010|30|000|   |000|001|"
                "+000000000017640|+0000000000500|+0000000000001|000000000000000\n"
                "010121000|00150|000|028|NO|11/2019|495|ZLC|006|GB|0|000|030|00|000|   |000|001|"
                "+000000000001795|+0000000000500|+0000000000001|000000000000000\n"
                "010121000|00150|000|028|NO|11/2019|495|ZLC|006|GB|0|000|030|30|000|   |000|001|"
                "+000000000045000|+0000000000500|+0000000000001|000000000000000\n"
                "999999999|0000000195241|0000000000000|0000000000060|0000000001286|+00000016612067849|"
                "+00000000047645952\n"
            ),
        )

    file2 = io.BytesIO()
    with zipfile.ZipFile(file2, "w") as zf:
        zf.writestr(
            "dummy_export_data",
            (
                "000000000|00000|000|HMCUSTOMS MONTHLY DATA| NOVEMBER|2019|NON-EU EXPORTS    \n"
                "010121010|230150|001|029|NO|12/2019|204|UMM|008|DK|0|000|010|30|000|   |000|001|"
                "+000000000017640|+0000000000500|+0000000000001|000000000000000\n"
                "999999999|0000000195241|0000000000000|0000000000060|0000000001286|+00000016612067849|"
                "+00000000047645952\n"
            ),
        )

    file3 = io.BytesIO()
    with zipfile.ZipFile(file3, "w") as zf:
        zf.writestr("dummy_export_data_1", file1.getvalue())
        zf.writestr("dummy_export_data_2", file2.getvalue())

    requests_mock.get("http://test/archive", content=html_archive.encode())
    requests_mock.get("http://test/latest", content=html_latest.encode())

    requests_mock.get("http://test/b/smke19_2019archive.zip", content=file3.getvalue())
    requests_mock.get("http://test/f/smke192002.zip", content=file1.getvalue())
    requests_mock.get("http://test/g/smke19_2020archive.zip", content=file3.getvalue())

    s3_mock = mock.MagicMock()
    mocker.patch.object(hmrc, "S3Data", return_value=s3_mock, autospec=True)
    mocker.patch.object(
        hmrc.config, "HMRC_UKTRADEINFO_ARCHIVE_URL", "http://test/archive"
    )
    mocker.patch.object(
        hmrc.config, "HMRC_UKTRADEINFO_LATEST_URL", "http://test/latest"
    )

    records_start_year = 2019
    run_date = datetime.datetime(2020, 3, 10).replace(tzinfo=pytz.UTC)

    hmrc.fetch_hmrc_trade_data(
        "non-eu-exports",
        "smke19",
        records_start_year,
        22,
        num_per_page=4,
        ts_nodash="task-1",
        run_date=run_date,
    )

    file_1_rows = [
        [
            "010121000",
            "00150",
            "000",
            "028",
            "NO",
            "11/2019",
            "204",
            "IMM",
            "008",
            "DK",
            "0",
            "000",
            "010",
            "30",
            "000",
            "   ",
            "000",
            "001",
            "+000000000017640",
            "+0000000000500",
            "+0000000000001",
            "000000000000000",
            "dummy_export_data",
        ],
        [
            "010121000",
            "00150",
            "000",
            "028",
            "NO",
            "11/2019",
            "495",
            "ZLC",
            "006",
            "GB",
            "0",
            "000",
            "030",
            "00",
            "000",
            "   ",
            "000",
            "001",
            "+000000000001795",
            "+0000000000500",
            "+0000000000001",
            "000000000000000",
            "dummy_export_data",
        ],
        [
            "010121000",
            "00150",
            "000",
            "028",
            "NO",
            "11/2019",
            "495",
            "ZLC",
            "006",
            "GB",
            "0",
            "000",
            "030",
            "30",
            "000",
            "   ",
            "000",
            "001",
            "+000000000045000",
            "+0000000000500",
            "+0000000000001",
            "000000000000000",
            "dummy_export_data",
        ],
    ]
    file_2_rows = [
        [
            "010121010",
            "230150",
            "001",
            "029",
            "NO",
            "12/2019",
            "204",
            "UMM",
            "008",
            "DK",
            "0",
            "000",
            "010",
            "30",
            "000",
            "   ",
            "000",
            "001",
            "+000000000017640",
            "+0000000000500",
            "+0000000000001",
            "000000000000000",
            "dummy_export_data",
        ]
    ]
    s3_mock.write_key.assert_has_calls(
        [
            mock.call("0000000000.json", file_1_rows + file_2_rows),
            mock.call("0000000001.json", file_1_rows + file_2_rows),
            mock.call("0000000002.json", file_1_rows),
        ]
    )
