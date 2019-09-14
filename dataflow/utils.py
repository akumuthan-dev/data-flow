"""A module that defines useful utils."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


def get_defined_pipeline_classes_by_key(module, key):
    """Return all class objects in given module which contains given key in its name."""
    return [
        cls for name, cls in module.__dict__.items()
        if isinstance(cls, type) and key in name
    ]


class XCOMIntegratedPostgresOperator(PostgresOperator):
    """Custom PostgresOperator which push query result into XCOM."""

    def execute(self, context):
        """Return execution result."""
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        return self.hook.get_records(self.sql, parameters=self.parameters)
