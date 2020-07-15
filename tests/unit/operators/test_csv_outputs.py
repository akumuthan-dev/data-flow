from datetime import datetime
from io import BytesIO

from unittest import mock
from zipfile import ZipFile

import pytest

from dataflow.operators import csv_outputs


@pytest.mark.parametrize(
    'timestamp_file,output_filename',
    [(True, 'test_base_file-2020-01-01.csv'), (False, 'test_base_file.csv')],
)
def test_create_csv(mocker, mock_db_conn, timestamp_file, output_filename):
    mocker.patch.object(csv_outputs.config, 'DATA_WORKSPACE_S3_BUCKET', 'test-bucket')
    mock_tmpfile = mocker.patch(
        'dataflow.operators.csv_outputs.tempfile.NamedTemporaryFile'
    )
    mock_tmpfile().__enter__().name = '/tmp/tmp1'
    mock_db_conn.execution_options().execute().fetchmany.side_effect = [
        [[1, 'name1'], [2, 'name2']],
        [[3, 'name3'], [4, 'name4']],
        None,
    ]
    mock_db_conn.execution_options().execute().keys.return_value = ['id', 'name']

    mock_s3 = mocker.patch('dataflow.operators.csv_outputs.S3Hook')

    mock_csv = mocker.patch('dataflow.operators.csv_outputs.csv')

    csv_outputs.create_csv(
        'testdb',
        'test_base_file',
        timestamp_file,
        'select * from tmp',
        execution_date=datetime(2019, 12, 1),
        next_execution_date=datetime(2020, 1, 1),
    )

    mock_csv.writer().writerow.assert_has_calls(
        [
            mock.call(['id', 'name']),
            mock.call([1, 'name1']),
            mock.call([2, 'name2']),
            mock.call([3, 'name3']),
            mock.call([4, 'name4']),
        ]
    )

    mock_s3.assert_has_calls(
        [
            mock.call('DATA_WORKSPACE_S3'),
            mock.call().load_file(
                '/tmp/tmp1',
                f's3://csv-pipelines/test_base_file/{output_filename}',
                bucket_name='test-bucket',
                replace=True,
            ),
        ]
    )


@pytest.mark.parametrize(
    'timestamp_file,output_filename',
    [(True, 'test_base_file-2020-01-01.csv'), (False, 'test_base_file.csv')],
)
def test_create_compressed_csv(mocker, mock_db_conn, timestamp_file, output_filename):
    mocker.patch.object(csv_outputs.config, 'DATA_WORKSPACE_S3_BUCKET', 'test-bucket')
    mock_tmpfile = mocker.patch(
        'dataflow.operators.csv_outputs.tempfile.NamedTemporaryFile',
    )
    mock_tmpfile().__enter__().name = '/tmp/tmp1'
    mock_db_conn.execution_options().execute().fetchmany.side_effect = [
        [[1, 'name1'], [2, 'name2']],
        [[3, 'name3'], [4, 'name4']],
        None,
    ]
    mock_db_conn.execution_options().execute().keys.return_value = ['id', 'name']

    mock_s3 = mocker.patch('dataflow.operators.csv_outputs.S3Hook')

    mock_csv = mocker.patch('dataflow.operators.csv_outputs.csv')

    csv_outputs.create_compressed_csv(
        'testdb',
        'test_base_file',
        timestamp_file,
        'select * from tmp',
        execution_date=datetime(2019, 12, 1),
        next_execution_date=datetime(2020, 1, 1),
    )

    mock_csv.writer().writerow.assert_has_calls(
        [
            mock.call(['id', 'name']),
            mock.call([1, 'name1']),
            mock.call([2, 'name2']),
            mock.call([3, 'name3']),
            mock.call([4, 'name4']),
        ]
    )

    b = BytesIO()
    for data in mock_tmpfile().__enter__().write.call_args_list:
        b.write(data[0][0])
    b.flush()

    zipfile = ZipFile(b)
    assert len(zipfile.filelist) == 1
    assert zipfile.filelist[0].filename == output_filename

    mock_s3.assert_has_calls(
        [
            mock.call('DATA_WORKSPACE_S3'),
            mock.call().load_file(
                '/tmp/tmp1',
                f's3://csv-pipelines/test_base_file/{output_filename}.zip',
                bucket_name='test-bucket',
                replace=True,
            ),
        ]
    )
