"""A module that defines useful utils."""

import json
import logging
from dataclasses import dataclass
from itertools import chain
from json import JSONEncoder
from typing import Any, Tuple, Union, Iterable, Dict, Optional, Sequence

import backoff
import botocore.exceptions
import sqlalchemy
from airflow.utils.state import State
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from cached_property import cached_property
from typing_extensions import Protocol

from dataflow import config


logger = logging.getLogger('dataflow')


# This describes how a blob of data relates to our desired DB structure. This is generally just in a single table,
# but if the response contains nested records then the FieldMapping can also include further `TableConfig` instances
# to describe the shape of the related tables.
FieldMapping = Sequence[
    Tuple[
        Union[str, int, Tuple[Union[str, int], ...], None],
        Union[sqlalchemy.Column, "TableConfig"],
    ]
]

# The below SingleTableFieldMapping describes only a single table, so only contains columns. It should not have
# any other `TableConfig`s.
SingleTableFieldMapping = Sequence[
    Tuple[Union[str, int, Tuple[Union[str, int], ...], None], sqlalchemy.Column]
]

TableMapping = Sequence[
    Tuple[Union[str, int, Tuple[Union[str, int], ...], None], "TableConfig"]
]


class Transform(Protocol):
    def __call__(
        self, record: Dict, table_config: "TableConfig", contexts: Tuple[Dict, ...]
    ) -> Dict:
        ...


@dataclass
class TableConfig:
    """Wrapper for all the information needed to define how data (e.g. from an API) should be processed and
    ingested into a database, including which fields to pull out and any transformations to apply.
    """

    table_name: str

    # A list of (field/path, Column/TableConfig) pairs, defining which source data
    # keys end up in which DB columns. Each field/path can be one of:
    # * None (indicating an autopopulated field like an auto-increment primary key))
    # * A string for a top-level key access
    # * A tuple of strings or integers for a nested key access
    #
    # If the second element is another TableConfig, it implies that the first element points to an iterable which
    # should be pulled into an additional table with the field mapping specified in that TableConfig. If sub-resources
    # do not have a reference to the main record, you'll need to apply a custom transform to pull that data in.
    # See DataHubSPIPipeline for an example.
    field_mapping: FieldMapping

    transforms: Iterable[Transform] = tuple()
    temp_table_suffix: Optional[str] = None

    _table = None
    _temp_table = None

    @cached_property
    def columns(self) -> SingleTableFieldMapping:
        return [
            pair for pair in self.field_mapping if not isinstance(pair[1], TableConfig)
        ]

    @cached_property
    def related_table_configs(self,) -> TableMapping:
        """Return directly-related TableConfigs, as a sequence of (key, TableConfig) pairs"""
        return [
            (key, column_or_table_config)
            for key, column_or_table_config in self.field_mapping
            if isinstance(column_or_table_config, TableConfig)
        ]

    @property
    def tables(self) -> Sequence[sqlalchemy.Table]:
        """Return all tables this TableConfig has information about, including this instance's table as well as
        deeply related tables.
        """
        return [self.table] + list(
            chain.from_iterable(
                table_config.tables for key, table_config in self.related_table_configs
            )
        )

    @property
    def table(self) -> sqlalchemy.Table:
        """Returns the immediate table this TableConfig has information about, excluding any related tables."""
        if self._table is None:
            self._table = sqlalchemy.Table(
                self.table_name,
                sqlalchemy.MetaData(),
                *[column.copy() for _, column in self.columns],
            )
        return self._table

    @property
    def temp_table(self):
        if not self.temp_table_suffix:
            raise RuntimeError(
                "`temp_table_suffix` is not set. "
                "You need to call `table_config.configure(**kwargs)` before accessing this attribute."
            )

        if self._temp_table is None:
            self._temp_table = sqlalchemy.Table(
                f"{self.table.name}_{self.temp_table_suffix}".lower(),
                self.table.metadata,
                *[column.copy() for column in self.table.columns],
            )

        return self._temp_table

    def configure(self, **kwargs):
        self.temp_table_suffix = kwargs["ts_nodash"]

        if self.related_table_configs:
            for _, related_table in self.related_table_configs:
                related_table.configure(**kwargs)


def slack_alert(context, success=False):
    if not config.SLACK_TOKEN:
        logger.info("No Slack token, skipping Slack notification")
        return

    dag_text = context['dag'].dag_id
    ts = context["ts"]
    title = "DAG run {}".format("succeeded" if success else "failed")

    if not success:
        failed_task_logs_url = (
            context['dag_run'].get_task_instances(state=State.FAILED)[0].log_url
        )
        failed_task_name = (
            context['dag_run'].get_task_instances(state=State.FAILED)[0].task_id
        )
        dag_text = f"<{failed_task_logs_url}|{dag_text}.{failed_task_name}>"

    return SlackWebhookHook(
        webhook_token=config.SLACK_TOKEN,
        attachments=[
            {
                "color": "#007000" if success else "#D2222D",
                "fallback": title,
                "blocks": [
                    {
                        "type": "section",
                        "text": {"text": title, "type": "mrkdwn"},
                        "fields": [
                            {"type": "mrkdwn", "text": "*DAG*"},
                            {"type": "mrkdwn", "text": "*Run*"},
                            {"type": "mrkdwn", "text": dag_text},
                            {"type": "plain_text", "text": ts},
                        ],
                    }
                ],
            }
        ],
    ).execute()


class DateTimeJsonEncoder(JSONEncoder):
    def default(self, o):
        try:
            # Date / DateTime objects both have `.isoformat()` methods.
            return o.isoformat()
        except AttributeError:
            pass

        # Let the base class default method raise the TypeError
        super().default(self, o)


class S3Data:
    def __init__(self, table_name, ts_nodash):
        self.client = S3Hook("DEFAULT_S3")
        self.bucket = config.S3_IMPORT_DATA_BUCKET
        self.prefix = f"import-data/{table_name}/{ts_nodash}/"

    @backoff.on_exception(
        backoff.expo, botocore.exceptions.EndpointConnectionError, max_tries=5
    )
    def write_key(self, key, data, jsonify=True):
        if jsonify:
            data = json.dumps(data, cls=DateTimeJsonEncoder)

        return self.client.load_string(
            data, self.prefix + key, bucket_name=self.bucket, replace=True, encrypt=True
        )

    def iter_keys(self, json=True):
        for key in self.list_keys():
            yield key, self.read_key(key, jsonify=json)

    @backoff.on_exception(
        backoff.expo, botocore.exceptions.EndpointConnectionError, max_tries=5
    )
    def list_keys(self):
        return self.client.list_keys(bucket_name=self.bucket, prefix=self.prefix) or []

    @backoff.on_exception(
        backoff.expo, botocore.exceptions.EndpointConnectionError, max_tries=5
    )
    def read_key(self, full_key, jsonify=True):
        data = self.client.read_key(full_key, bucket_name=self.bucket)
        return json.loads(data) if jsonify else data


def get_nested_key(
    data: dict, path: Union[Tuple, str, int], required: bool = False
) -> Any:
    if isinstance(path, (str, int)):
        path = (path,)

    for key in path:
        try:
            data = data[key]
        except (KeyError, IndexError, TypeError):
            if required:
                raise
            else:
                return None
    return data
