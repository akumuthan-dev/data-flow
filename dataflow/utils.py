from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from dataflow import constants


def get_defined_pipeline_classes_by_key(module, key):
    return [
        cls for name, cls in module.__dict__.items()
        if isinstance(cls, type) and key in name
    ]


def get_first_day_of_financial_year():
    first_day, month = constants.FINANCIAL_YEAR_FIRST_DAY_MONTH.split('-')
    today = datetime.now().date()
    first_day_of_financial_year = today.replace(day=first_day, month=month)
    if today <= first_day_of_financial_year:
        first_day_of_financial_year = first_day_of_financial_year.replace(year=today.year-1)
    return first_day_of_financial_year


class XCOMIntegratedPostgresOperator(PostgresOperator):

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        return self.hook.get_records(self.sql, parameters=self.parameters)
