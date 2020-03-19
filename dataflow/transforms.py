from typing import Tuple, Dict

from dataflow.utils import TableConfig


def drop_empty_string_fields(
    record: dict, table_config: TableConfig, contexts: Tuple[Dict, ...]
) -> Dict:
    """Remove keys (and nested keys) from this dictionary where the value is an empty string
    """

    def _drop_keys_with_empty_string_values(obj):
        if type(obj) is dict:
            return {
                key: _drop_keys_with_empty_string_values(value)
                for key, value in obj.items()
                if value != ""
            }

        return obj

    return _drop_keys_with_empty_string_values(record)
