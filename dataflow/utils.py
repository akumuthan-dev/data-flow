"""A module that defines useful utils."""

import json
import logging
from typing import Any, List, Tuple, Union

import sqlalchemy
from airflow.hooks.S3_hook import S3Hook

from dataflow import config


FieldMapping = List[
    Tuple[Union[str, Tuple[Union[str, int], ...], None], sqlalchemy.Column]
]


logger = logging.getLogger('dataflow')


class S3Data:
    def __init__(self, table_name, ts_nodash):
        self.client = S3Hook("DEFAULT_S3")
        self.bucket = config.S3_IMPORT_DATA_BUCKET
        self.prefix = f"import-data/{table_name}/{ts_nodash}/"

    def write_key(self, key, data, jsonify=True):
        if jsonify:
            data = json.dumps(data)

        return self.client.load_string(
            data, self.prefix + key, bucket_name=self.bucket, replace=True, encrypt=True
        )

    def iter_keys(self, json=True):
        for key in self.list_keys():
            yield key, self.read_key(key, jsonify=json)

    def list_keys(self):
        return self.client.list_keys(bucket_name=self.bucket, prefix=self.prefix)

    def read_key(self, full_key, jsonify=True):
        data = self.client.read_key(full_key, bucket_name=self.bucket)
        return json.loads(data) if jsonify else data


def get_nested_key(data: dict, path: Union[Tuple, str], required: bool = False) -> Any:
    if isinstance(path, str):
        path = (path,)

    for key in path:
        try:
            data = data[key]
        except (KeyError, IndexError):
            if required:
                raise
            else:
                return None
    return data
