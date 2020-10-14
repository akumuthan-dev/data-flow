from unittest import mock

from dataflow.dags.covid19_pipelines import AppleCovid19MobilityTrendsPipeline
from dataflow.operators import covid19


def test_fetch_apple_mobility_data(mocker, requests_mock):
    s3_mock = mock.Mock()

    mocker.patch.object(covid19, 'S3Data', return_value=s3_mock, autospec=True)

    requests_mock.get(
        'http://test/config',
        json={'basePath': '/base/', 'regions': {'en-us': {'csvPath': 'test.csv'}}},
    )

    requests_mock.get(
        "http://test/base/test.csv",
        text=(
            'geo_type,region,transportation_type,alternative_name,sub-region,country,2020-01-13,2020-01-14\n'
            'country/region,Australia,transit,AU,,,100,101.78\n'
        ),
    )

    covid19.fetch_apple_mobility_data(
        table_name='test',
        base_url='http://test',
        config_path='/config',
        df_transform=AppleCovid19MobilityTrendsPipeline().transform_dataframe,
        ts_nodash="123",
    )
    # raise Exception(s3_mock.write_key.call_args_list)
    s3_mock.write_key.assert_has_calls(
        [
            mock.call(
                '0000000001.json',
                '[{"geo_type":"country\\/region","region":"Australia","sub-region":null,"country":"Australia",'
                '"date":"2020-01-13","transit":0.0,"adm_area_1":null,"adm_area_2":null},'
                '{"geo_type":"country\\/region","region":"Australia","sub-region":null,"country":"Australia",'
                '"date":"2020-01-14","transit":1.78,"adm_area_1":null,"adm_area_2":null}]',
                jsonify=False,
            )
        ]
    )
