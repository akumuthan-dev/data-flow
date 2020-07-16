import pytest

from dataflow.transforms import drop_empty_string_fields
from dataflow.utils import TableConfig


@pytest.mark.parametrize(
    "input_data, expected_data",
    (
        ({"key": "value"}, {"key": "value"}),
        ({"key": "value", "blank": ""}, {"key": "value"}),
        ({"key": "value", "empty_list": []}, {"key": "value", "empty_list": []}),
        ({"key": "value", "empty_dict": {}}, {"key": "value", "empty_dict": {}}),
        ({"key": "value", "space": " "}, {"key": "value", "space": " "}),
        (
            {"key": "value", "nested": {"key": "value"}},
            {"key": "value", "nested": {"key": "value"}},
        ),
        (
            {"key": "value", "nested": {"key": "value", "blank": ""}},
            {"key": "value", "nested": {"key": "value"}},
        ),
    ),
)
def test_drop_empty_string_fields(input_data, expected_data):
    table_config = TableConfig(table_name="foo", field_mapping=[])
    assert drop_empty_string_fields(input_data, table_config, tuple()) == expected_data
