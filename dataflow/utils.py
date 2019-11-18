"""A module that defines useful utils."""

from typing import Any, List, Tuple, Union

import redis
import sqlalchemy
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import config


FieldMapping = List[Tuple[Union[str, Tuple], sqlalchemy.Column]]


def get_redis_client():
    """Returns redis client from connection URL"""
    return redis.from_url(url=config.REDIS_URL, decode_responses=True)


def get_nested_key(data: dict, path: Union[Tuple, str], required: bool = False) -> Any:
    if isinstance(path, str):
        path = (path,)

    for key in path:
        data = data[key] if required else data.get(key)
        if not required and data is None:
            return data

    return data


class XCOMIntegratedPostgresOperator(PostgresOperator):
    """Custom PostgresOperator which push query result into XCOM."""

    def execute(self, context):
        """Return execution result."""
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        return self.hook.get_records(self.sql, parameters=self.parameters)
