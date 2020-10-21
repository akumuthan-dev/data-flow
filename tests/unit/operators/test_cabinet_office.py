from unittest import mock

import freezegun

from dataflow.operators import cabinet_office


def test_fetch_gender_pay_gap_files(mocker):
    mock_fetch_csvs = mocker.patch(
        'dataflow.operators.cabinet_office.fetch_mapped_hosted_csvs'
    )
    transform = mock.Mock()

    with freezegun.freeze_time('2020-09-01T12:00:00Z'):
        cabinet_office.fetch_gender_pay_gap_files(
            'gender_pay_gap',
            'http://test/{year}',
            2018,
            transform,
            ts_nodash="123",
        )

    mock_fetch_csvs.fetch_called_once_with(
        'gender_pay_gap',
        {
            '2018': 'http://test/2018',
            '2019': 'http://test/2019',
            '2020': 'http://test/2020',
        },
        transform,
        ts_nodash='123',
    )
