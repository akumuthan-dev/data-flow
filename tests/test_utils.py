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
