from unittest import mock

import pytest
from sqlalchemy import ARRAY, Column, Integer, Text, DateTime

import dataflow
from dataflow.operators import dss_generic
from dataflow.utils import TableConfig


def test_get_table_config(mocker):
    api_response = {
        'columns': [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'batch_ref', 'type': 'TEXT'},
            {'name': 'status', 'type': 'TEXT'},
            {'name': 'start_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
            {'name': 'int_list', 'type': 'INTEGER[]'},
            {'name': 'text_list', 'type': 'TEXT[]'},
            {'name': 'timestamp_list', 'type': 'TIMESTAMP WITHOUT TIME ZONE[]'},
        ],
    }
    config = {
        'data_uploader_table_name': 'L0',
        'data_uploader_schema_name': 'test_org.test_dataset',
    }
    request, context = _patch_get_table_config(mocker, api_response, config)
    table_config = dss_generic.get_table_config(**context)

    assert table_config.table_name == 'test_dataset'
    assert table_config.schema == 'test_org'

    field_mapping_types = [field[1].type for field in table_config.field_mapping]
    expected_field_mapping_types = [Integer] + [Text] * 2 + [DateTime] + [ARRAY] * 3

    for i, type in enumerate(field_mapping_types):
        expected_type = expected_field_mapping_types[i]
        assert isinstance(type, expected_type)

    request.assert_called_once_with(
        credentials={'id': 'test_id', 'key': 'test_key', 'algorithm': 'sha256'},
        next_key=None,
        results_key='columns',
        url='http://test/api/v1/table-structure/test_org.test_dataset/L0',
    )


def test_get_table_config_quotes_schema_and_table_name(mocker):
    api_response = {'columns': []}
    config = {
        'data_uploader_table_name': 'L0',
        'data_uploader_schema_name': 'bad schema.name',
    }
    request, context = _patch_get_table_config(mocker, api_response, config)
    dss_generic.get_table_config(**context)

    request.assert_called_once_with(
        credentials={'id': 'test_id', 'key': 'test_key', 'algorithm': 'sha256'},
        next_key=None,
        results_key='columns',
        url='http://test/api/v1/table-structure/bad%20schema.name/L0',
    )


def test_get_table_config_invalid_schema(mocker):
    api_response = {
        'columns': [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'batch_ref', 'type': 'TEXT'},
            {'name': 'status', 'type': 'TEXT'},
            {'name': 'start_date', 'type': 'TIMESTAMP WITHOUT TIME ZONE'},
        ],
    }
    config = {
        'data_uploader_table_name': 'L0',
        'data_uploader_schema_name': 'test_org',
    }
    request, context = _patch_get_table_config(mocker, api_response, config)

    with pytest.raises(ValueError):
        dss_generic.get_table_config(**context)


def test_get_table_config_unsupported_data_type(mocker):
    api_response = {
        'columns': [{'name': 'id', 'type': 'NONEXISTANT'}],
    }
    config = {
        'data_uploader_table_name': 'L0',
        'data_uploader_schema_name': 'test_org.test_dataset',
    }
    request, context = _patch_get_table_config(mocker, api_response, config)

    with pytest.raises(ValueError):
        dss_generic.get_table_config(**context)


def _patch_get_table_config(mocker, api_response, config):
    mocker.patch.object(
        dataflow.operators.dss_generic, "DATA_STORE_SERVICE_BASE_URL", "http://test"
    )
    mocker.patch.object(
        dataflow.operators.dss_generic,
        "DATA_STORE_SERVICE_HAWK_CREDENTIALS",
        {'id': 'test_id', 'key': 'test_key', 'algorithm': 'sha256'},
    )
    request = mocker.patch.object(
        dataflow.operators.dss_generic,
        '_hawk_api_request',
        return_value=api_response,
        autospec=True,
    )

    dag_run = mock.MagicMock()
    dag_run.conf = config
    context = {'dag_run': dag_run}
    return request, context


def test_fetch_data(mocker):
    mocker.patch.object(
        dataflow.operators.dss_generic, "DATA_STORE_SERVICE_BASE_URL", "http://test"
    )
    mocker.patch.object(
        dataflow.operators.dss_generic,
        "DATA_STORE_SERVICE_HAWK_CREDENTIALS",
        {'id': 'test_id', 'key': 'test_key', 'algorithm': 'sha256'},
    )
    req = mocker.patch.object(
        dataflow.operators.dss_generic,
        'fetch_from_hawk_api',
        return_value={'results': [{'id': 1, 'text': 'foo bar'}], 'next': None},
        autospec=True,
    )

    s3_mock = mock.MagicMock()
    mocker.patch.object(
        dataflow.operators.common, "S3Data", return_value=s3_mock, autospec=True
    )

    table_config = TableConfig(
        table_name='test_dataset',
        field_mapping=[
            ('id', Column('id', Integer())),
            ('batch_ref', Column('batch_ref', Text())),
            ('status', Column('status', Text())),
            ('start_date', Column('start_date', DateTime())),
        ],
        transforms=(),
        temp_table_suffix=None,
        schema='test_org',
    )

    task_instance = mock.MagicMock()
    task_instance.xcom_pull.return_value = table_config
    dag_run = mock.MagicMock()
    dag_run.conf = {
        'data_uploader_table_name': 'L0',
        'data_uploader_schema_name': 'test_org.test_dataset',
    }
    context = {
        'dag_run': dag_run,
        'task_instance': task_instance,
    }
    dss_generic.fetch_data(**context)

    req.assert_called_once_with(
        hawk_credentials={'id': 'test_id', 'key': 'test_key', 'algorithm': 'sha256'},
        source_url='http://test/api/v1/table-data/test_org.test_dataset/L0?orientation=records',
        table_name='test_dataset',
        **context
    )
