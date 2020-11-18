from unittest import mock

import pytest

from dataflow.dags import maintenance


@pytest.fixture
def s3(mocker):
    s3_mock = mock.MagicMock()
    mocker.patch.object(maintenance, "S3Hook", return_value=s3_mock, autospec=True)

    return s3_mock


@pytest.fixture
def postgres_hook(mocker):
    return mocker.patch.object(maintenance, "PostgresHook")


def test_cleanup_old_s3_files(s3):
    s3.list_prefixes.side_effect = [
        ['pipeline1/', 'pipeline2/', 'pipeline3/'],
        ['pipeline1/20000101T000000/', 'pipeline1/20000104T000000/'],
        ['pipeline2/20000101T000000/', 'pipeline2/20000104T000000/'],
        ['pipeline3/20000101T000000/'],
    ]

    maintenance.cleanup_old_s3_files(ts_nodash="20000110T000000")

    assert s3.get_bucket().objects.filter.mock_calls == [
        mock.call(Prefix='pipeline1/20000101T000000/'),
        mock.call().delete(),
        mock.call(Prefix='pipeline2/20000101T000000/'),
        mock.call().delete(),
    ]


def test_cleanup_old_dataset_db_tables(mock_db_conn):
    mock_db_conn.execute.return_value = [
        ('public', 'table1',),
        ('public', 'table1_20000101t000000',),
        ('public', 'table1_20000101t000000_swap',),
        ('public', 'table1_20000108t000000',),
        ('public', 'table1_20000108t000000_swap',),
        ('public', 'table2_20000101t000000',),
        ('dit', 'table3',),
        ('dit', 'table3_20000101t000000',),
    ]

    maintenance.cleanup_old_datasets_db_tables(ts_nodash="20000110T000000")

    assert mock_db_conn.execute.mock_calls == [
        mock.call(
            '''
SELECT schemaname, tablename
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN ('dataflow', 'information_schema')
AND schemaname NOT LIKE '\\_%%'
AND schemaname NOT LIKE 'pg_%%'
'''
        ),
        mock.call("DROP TABLE QUOTED<public>.QUOTED<table1_20000101t000000>"),
        mock.call("DROP TABLE QUOTED<public>.QUOTED<table1_20000101t000000_swap>"),
        mock.call("DROP TABLE QUOTED<dit>.QUOTED<table3_20000101t000000>"),
    ]
