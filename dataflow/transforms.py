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


def transform_ons_marker_field(
    record: dict, table_config: TableConfig, contexts: Tuple[Dict, ...]
) -> dict:
    """
    Formats the ons trade marker field for consumption by analysts, specifically:

    1. transform empty string to null
    2. replaces 'not-applicable' with 'not-available'
    """
    marker = None
    if 'marker' in record:
        marker = record['marker']['value']
    elif 'Marker' in record:
        marker = record['Marker']

    if marker == '':
        marker = None
    elif marker == 'not-applicable' or marker == 'N/A':
        marker = 'not-available'

    return {
        **record,
        'norm_marker': marker,
    }
