import io
import zipfile
from datetime import datetime
from unittest import mock

from dataflow.operators import companies_house


def test_fetch_companies(mocker, requests_mock):
    file1 = io.BytesIO()
    with zipfile.ZipFile(file1, "w") as zf:
        zf.writestr(
            'dummy_companies',
            'field1,field2,field3\ntext1,010121000,00150\ntext2,010121000,00150\ntext3,010121000,00150\n',
        )

    file2 = io.BytesIO()
    with zipfile.ZipFile(file2, "w") as zf:
        zf.writestr(
            'dummy_companies',
            'field1,field2,field3\ntext3,000000000,0000000195241\ntext4,999999999,00000\n',
        )

    requests_mock.get(
        'http://test/BasicCompanyData-2020-01-01-part1_2.zip', content=file1.getvalue(),
    )

    requests_mock.get(
        'http://test/BasicCompanyData-2020-01-01-part2_2.zip', content=file2.getvalue(),
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(companies_house, "S3Data", return_value=s3_mock, autospec=True)

    companies_house.fetch_companies_house_companies(
        'companies',
        'http://test/BasicCompanyData-{file_date}-part{file_num}_{num_files}.zip',
        2,
        page_size=3,
        ts_nodash='task-1',
        next_execution_date=datetime(2020, 1, 10),
    )

    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                [
                    {
                        'field1': 'text1',
                        'field2': '010121000',
                        'field3': '00150',
                        'publish_date': '2020-01-01',
                    },
                    {
                        'field1': 'text2',
                        'field2': '010121000',
                        'field3': '00150',
                        'publish_date': '2020-01-01',
                    },
                    {
                        'field1': 'text3',
                        'field2': '010121000',
                        'field3': '00150',
                        'publish_date': '2020-01-01',
                    },
                ],
            ),
            mock.call(
                '0000000002.json',
                [
                    {
                        'field1': 'text3',
                        'field2': '000000000',
                        'field3': '0000000195241',
                        'publish_date': '2020-01-01',
                    },
                    {
                        'field1': 'text4',
                        'field2': '999999999',
                        'field3': '00000',
                        'publish_date': '2020-01-01',
                    },
                ],
            ),
        ]
    )
