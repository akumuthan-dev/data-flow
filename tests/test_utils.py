import pytest

from dataflow import utils


@pytest.mark.parametrize(
    "path, required, expected",
    [
        ("e", True, 5),
        (("e",), True, 5),
        (("a", "b", "c"), True, 1),
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


def test_s3_data_read_keys(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")

    mock_read = mocker.patch('dataflow.utils.S3Hook.read_key', return_value='["data"]')

    s3 = utils.S3Data('table', '20010101')

    assert s3.read_key("prefix/key.json") == ["data"]

    mock_read.assert_called_once_with("prefix/key.json", bucket_name="test-bucket")


def test_s3_data_iter_keys(mocker):
    mocker.patch.object(utils.config, "S3_IMPORT_DATA_BUCKET", "test-bucket")
    mock_list = mocker.patch(
        'dataflow.utils.S3Hook.list_keys', return_value=["key1", "key2"]
    )
    mocker.patch('dataflow.utils.S3Hook.read_key', return_value='"data"')

    s3 = utils.S3Data('table', '20010101')
    assert list(s3.iter_keys("prefix/key.json")) == [("key1", "data"), ("key2", "data")]

    mock_list.assert_called_once_with(
        bucket_name="test-bucket", prefix='import-data/table/20010101/'
    )
