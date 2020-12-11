import decimal
from datetime import date, datetime
from io import BytesIO

import botocore.exceptions
import freezegun as freezegun
import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, String

from dataflow import utils
from dataflow.utils import TableConfig


@pytest.mark.parametrize(
    "path, required, expected",
    [
        ("e", True, 5),
        (("e",), True, 5),
        (("a", "b", "c"), True, 1),
        (("a", lambda data: data['b'], "c"), True, 1),
        (("a", "l", 0), True, 2),
        ("f", False, None),
        (("g", "h", "i"), False, None),
    ],
)
def test_nested_key(path, required, expected):
    data = {"a": {"b": {"c": 1}, "l": [2, 3, 4]}, "e": 5}
    assert utils.get_nested_key(data, path, required) == expected


def test_s3_data(mocker):

    s3 = utils.S3Data('table', '20010101')

    assert s3.prefix == 'import-data/table/20010101/'


def test_s3_data_write_key(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mock_load = mocker.patch('dataflow.utils.S3Hook.load_string')

    s3 = utils.S3Data('table', '20010101')
    s3.write_key("key.json", {"data": "value"})

    mock_load.assert_called_once_with(
        '{"data": "value"}',
        'import-data/table/20010101/key.json',
        bucket_name="test-bucket",
        encrypt=True,
        replace=True,
    )


def test_s3_data_write_key_handles_dates_and_times(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mock_load = mocker.patch('dataflow.utils.S3Hook.load_string')

    s3 = utils.S3Data('table', '20010101')
    with freezegun.freeze_time('2020-01-01T12:00:00Z'):
        s3.write_key("key.json", {"date": date.today(), "datetime": datetime.now()})

    mock_load.assert_called_once_with(
        '{"date": "2020-01-01", "datetime": "2020-01-01T12:00:00"}',
        'import-data/table/20010101/key.json',
        bucket_name="test-bucket",
        encrypt=True,
        replace=True,
    )


def test_s3_data_write_key_handles_decimals(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mock_load = mocker.patch('dataflow.utils.S3Hook.load_string')

    s3 = utils.S3Data('table', '20020202')
    with freezegun.freeze_time('2020-01-01T12:00:00Z'):
        s3.write_key("key.json", {'a_decimal': decimal.Decimal('1.11')})

    mock_load.assert_called_once_with(
        '{"a_decimal": "1.11"}',
        'import-data/table/20020202/key.json',
        bucket_name="test-bucket",
        encrypt=True,
        replace=True,
    )


def test_s3_data_read_keys(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")
    mock_read = mocker.patch(
        'dataflow.utils.S3Hook.get_key',
        return_value=mocker.Mock(
            get=mocker.Mock(
                return_value={'Body': BytesIO(b'[{"key":"val_1"},{"key":"val_2"}]')}
            )
        ),
    )

    s3 = utils.S3Data('table', '20010101')

    assert list(s3.read_key("prefix/key.json")) == [{"key": "val_1"}, {"key": "val_2"}]

    mock_read.assert_called_once_with("prefix/key.json", bucket_name="test-bucket")


def test_s3_data_iter_keys(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")
    mock_list = mocker.patch(
        'dataflow.utils.S3Hook.list_keys', return_value=iter(["key1", "key2"])
    )
    mocker.patch(
        'dataflow.utils.S3Hook.get_key',
        return_value=mocker.Mock(
            get=mocker.Mock(
                side_effect=lambda: {
                    'Body': BytesIO(b'[{"key":"val_1"},{"key":"val_2"}]')
                }
            )
        ),
    )

    s3 = utils.S3Data('table', '20010101')
    records = [
        key_record
        for (key, key_records) in s3.iter_keys()
        for key_record in key_records
    ]

    assert records == [{"key": "val_1"}, {"key": "val_2"}] * 2

    mock_list.assert_called_once_with(
        bucket_name="test-bucket", prefix='import-data/table/20010101/'
    )


def test_s3_data_read_key_retries_requests(mocker):
    mocker.patch("time.sleep")  # skip backoff retry delay
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mocker.patch(
        'dataflow.utils.S3Hook.get_key',
        side_effect=[
            botocore.exceptions.EndpointConnectionError(endpoint_url='aws'),
            mocker.Mock(
                get=mocker.Mock(
                    side_effect=lambda: {
                        'Body': BytesIO(b'[{"key":"val_1"},{"key":"val_2"}]')
                    }
                )
            ),
        ],
    )

    s3 = utils.S3Data('table', '20010101')

    assert list(s3.read_key("key")) == [{"key": "val_1"}, {"key": "val_2"}]


def test_s3_data_write_key_retries_requests(mocker):
    mocker.patch("time.sleep")  # skip backoff retry delay
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mocker.patch(
        'dataflow.utils.S3Hook.load_string',
        side_effect=[
            botocore.exceptions.EndpointConnectionError(endpoint_url='aws'),
            "{}",
        ],
    )

    s3 = utils.S3Data('table', '20010101')

    assert s3.write_key("key", {})


def test_s3_data_list_keys_retries_requests(mocker):
    mocker.patch("time.sleep")  # skip backoff retry delay
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mocker.patch(
        'dataflow.utils.S3Hook.list_keys',
        side_effect=[
            botocore.exceptions.EndpointConnectionError(endpoint_url='aws'),
            [],
        ],
    )

    s3 = utils.S3Data('table', '20010101')

    assert s3.list_keys() == []


@pytest.mark.parametrize(
    "table_name, extra_path, expected",
    [
        ("table", None, "upstream/table/"),
        ("table", "", "upstream/table/"),
        ("table", "path", "upstream/table/path/"),
        ("table", "path/", "upstream/table/path/"),
        ("table", "/", "upstream/table//"),
    ],
)
def test_s3_upstream(table_name, extra_path, expected):
    s3 = utils.S3Upstream(table_name=table_name, extra_path=extra_path)
    assert s3.prefix == expected


class TestTableConfig:
    def test_columns_property_only_returns_immediate_columns(self):
        id_col = ("id", Column("id", Integer))
        name_col = ("name", Column("name", String))

        single_table_config = TableConfig(table_name="test", field_mapping=[id_col],)

        nested_table_config = TableConfig(
            table_name="test",
            field_mapping=[
                id_col,
                (
                    "relation",
                    TableConfig(
                        table_name="relation",
                        field_mapping=[("foo", Column("foo", String))],
                    ),
                ),
                name_col,
            ],
        )

        assert single_table_config.columns == [id_col]
        assert nested_table_config.columns == [id_col, name_col]

    def test_table_property_returns_sqlalchemy_table(self):
        single_table_config = TableConfig(
            table_name="test", field_mapping=[("id", Column("id", Integer))],
        )

        table = single_table_config.table
        assert type(table) is sqlalchemy.Table
        assert {col.name for col in table.columns} == {"id"}

    def test_tables_property_returns_all_tables(self):
        id_col = ("id", Column("id", Integer))
        name_col = ("name", Column("name", String))
        nested_table_config = TableConfig(
            table_name="test",
            field_mapping=[
                id_col,
                (
                    "relation",
                    TableConfig(
                        table_name="relation",
                        field_mapping=[("foo", Column("foo", String))],
                    ),
                ),
                name_col,
            ],
        )

        tables = nested_table_config.tables
        assert all(type(table) is sqlalchemy.Table for table in tables)
        assert [table.name for table in tables] == ["test", "relation"]
        assert {col.name for col in tables[0].columns} == {"id", "name"}
        assert {col.name for col in tables[1].columns} == {"foo"}

    def test_related_tables_property_returns_directly_related_tables(self):
        id_col = ("id", Column("id", Integer))
        foo_col = ("foo", Column("foo", Integer))
        bar_col = ("bar", Column("bar", Integer))
        config = TableConfig(
            table_name="test",
            field_mapping=[
                id_col,
                (
                    "foo",
                    TableConfig(
                        table_name="foo",
                        field_mapping=[
                            foo_col,
                            (
                                "bar",
                                TableConfig(table_name="bar", field_mapping=[bar_col]),
                            ),
                        ],
                    ),
                ),
            ],
        )

        assert len(config.related_table_configs) == 1
        assert config.related_table_configs[0][0] == "foo"
        assert type(config.related_table_configs[0][1]) is TableConfig
        assert config.related_table_configs[0][1].table_name == "foo"

        assert len(config.related_table_configs[0][1].related_table_configs)
        assert config.related_table_configs[0][1].related_table_configs[0][0] == "bar"
        assert (
            type(config.related_table_configs[0][1].related_table_configs[0][1])
            is TableConfig
        )
        assert (
            config.related_table_configs[0][1].related_table_configs[0][1].table_name
            == "bar"
        )
