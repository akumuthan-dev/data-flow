import io
import zipfile
from unittest import mock

import pandas

from dataflow.operators import comtrade


DUMMY_DATA_FILENAME = "SMKE191912.zip"


def test_fetch_goods_from_comtrade(requests_mock):
    requests_mock.get(
        "https://comtrade.un.org/Data/cache/years.json",
        json={
            'more': False,
            'results': [{"id": "2019", "text": "2019"}, {"id": "2018", "text": "2018"}],
        },
    )

    goods_csv_2019 = io.BytesIO()
    with zipfile.ZipFile(goods_csv_2019, "w") as zf:
        zf.writestr(
            "goods_data.csv",
            (
                "Classification,Year,Period,Period Desc.,Aggregate Level,Is Leaf Code,Trade Flow Code,Trade Flow,Reporter Code,Reporter,Reporter ISO,Partner Code,Partner,Partner ISO,Commodity Code,Commodity,Qty Unit Code,Qty Unit,Qty,Netweight (kg),Trade Value (US$),Flag\n"
                "S3,2008,2008,2008,1,0,1,Import,4,Afghanistan,AFG,0,World,WLD,0,Food and live animals,1,No Quantity,,,309541080,0\n"
            ),
        )

    requests_mock.get(
        "https://comtrade.un.org/api/get/bulk/c/A/2019/all/HS",
        content=goods_csv_2019.getvalue(),
    )

    goods_csv_2018 = io.BytesIO()
    with zipfile.ZipFile(goods_csv_2018, "w") as zf:
        zf.writestr(
            "goods_data_2.csv",
            (
                "Classification,Year,Period,Period Desc.,Aggregate Level,Is Leaf Code,Trade Flow Code,Trade Flow,Reporter Code,Reporter,Reporter ISO,Partner Code,Partner,Partner ISO,Commodity Code,Commodity,Qty Unit Code,Qty Unit,Qty,Netweight (kg),Trade Value (US$),Flag\n"
                "S3,2008,2008,2008,1,0,2,Export,4,Afghanistan,AFG,0,World,WLD,0,Food and live animals,1,No Quantity,,,280276257,0\n"
            ),
        )

    requests_mock.get(
        "https://comtrade.un.org/api/get/bulk/c/A/2018/all/HS",
        content=goods_csv_2018.getvalue(),
    )

    data_frames = list(comtrade.fetch_comtrade_goods_data_frames())

    expected_data_frames = [
        pandas.DataFrame(
            [
                {
                    'Classification': 'S3',
                    'Year': '2008',
                    'Period': '2008',
                    'Period Desc.': '2008',
                    'Aggregate Level': '1',
                    'Is Leaf Code': False,
                    'Trade Flow Code': '1',
                    'Trade Flow': 'Import',
                    'Reporter Code': '4',
                    'Reporter': 'Afghanistan',
                    'Reporter ISO': 'AFG',
                    'Partner Code': '0',
                    'Partner': 'World',
                    'Partner ISO': 'WLD',
                    'Commodity Code': '0',
                    'Commodity': 'Food and live animals',
                    'Qty Unit Code': '1',
                    'Qty Unit': 'No Quantity',
                    'Qty': None,
                    'Trade Value (US$)': '309541080',
                }
            ],
        ),
        pandas.DataFrame(
            [
                {
                    'Classification': 'S3',
                    'Year': '2008',
                    'Period': '2008',
                    'Period Desc.': '2008',
                    'Aggregate Level': '1',
                    'Is Leaf Code': False,
                    'Trade Flow Code': '2',
                    'Trade Flow': 'Export',
                    'Reporter Code': '4',
                    'Reporter': 'Afghanistan',
                    'Reporter ISO': 'AFG',
                    'Partner Code': '0',
                    'Partner': 'World',
                    'Partner ISO': 'WLD',
                    'Commodity Code': '0',
                    'Commodity': 'Food and live animals',
                    'Qty Unit Code': '1',
                    'Qty Unit': 'No Quantity',
                    'Qty': None,
                    'Trade Value (US$)': '280276257',
                }
            ]
        ),
    ]

    assert all(
        df.to_csv() == edf.to_csv()
        for df, edf in zip(data_frames, expected_data_frames)
    )


def test_fetch_services_from_comtrade(mocker, requests_mock):

    requests_mock.get(
        "https://comtrade.un.org/Data/cache/years.json",
        json={
            'more': False,
            'results': [{"id": "2019", "text": "2019"}, {"id": "2018", "text": "2018"}],
        },
    )

    services_csv_2019 = io.BytesIO()
    with zipfile.ZipFile(services_csv_2019, "w") as zf:
        zf.writestr(
            "services_data.csv",
            (
                "Classification,Year,Period,Period Desc.,Aggregate Level,Is Leaf Code,Trade Flow Code,Trade Flow,Reporter Code,Reporter,Reporter ISO,Partner Code,Partner,Partner ISO,Commodity Code,Commodity,Trade Value (US$),Flag\n"
                "EB,2008,2008,2008,0,0,1,Import,4,Afghanistan,,0,World,,200,Total EBOPS Services,570089358,0\n"
            ),
        )

    requests_mock.get(
        "https://comtrade.un.org/api/get/bulk/S/A/2019/all/EB02",
        content=services_csv_2019.getvalue(),
    )

    services_csv_2018 = io.BytesIO()
    with zipfile.ZipFile(services_csv_2018, "w") as zf:
        zf.writestr(
            "services_data_2.csv",
            (
                "Classification,Year,Period,Period Desc.,Aggregate Level,Is Leaf Code,Trade Flow Code,Trade Flow,Reporter Code,Reporter,Reporter ISO,Partner Code,Partner,Partner ISO,Commodity Code,Commodity,Trade Value (US$),Flag\n"
                "EB,2008,2008,2008,0,0,2,Import,4,Afghanistan,,0,World,,200,Total EBOPS Services,1217942038,0\n"
            ),
        )

    requests_mock.get(
        "https://comtrade.un.org/api/get/bulk/S/A/2018/all/EB02",
        content=services_csv_2018.getvalue(),
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(comtrade, "S3Data", return_value=s3_mock, autospec=True)

    comtrade.fetch_comtrade_services_data(
        "services", ts_nodash="task-1",
    )

    file_1_rows = [
        {
            'Classification': 'EB',
            'Year': '2008',
            'Period': '2008',
            'Period Desc.': '2008',
            'Aggregate Level': '0',
            'Is Leaf Code': '0',
            'Trade Flow Code': '1',
            'Trade Flow': 'Import',
            'Reporter Code': '4',
            'Reporter': 'Afghanistan',
            'Reporter ISO': None,
            'Partner Code': '0',
            'Partner': 'World',
            'Partner ISO': None,
            'Commodity Code': '200',
            'Commodity': 'Total EBOPS Services',
            'Trade Value (US$)': '570089358',
            'Flag': '0',
        },
        {
            'Classification': 'EB',
            'Year': '2008',
            'Period': '2008',
            'Period Desc.': '2008',
            'Aggregate Level': '0',
            'Is Leaf Code': '0',
            'Trade Flow Code': '2',
            'Trade Flow': 'Import',
            'Reporter Code': '4',
            'Reporter': 'Afghanistan',
            'Reporter ISO': None,
            'Partner Code': '0',
            'Partner': 'World',
            'Partner ISO': None,
            'Commodity Code': '200',
            'Commodity': 'Total EBOPS Services',
            'Trade Value (US$)': '1217942038',
            'Flag': '0',
        },
    ]

    s3_mock.write_key.assert_has_calls([mock.call("0000000000.json", file_1_rows)])
