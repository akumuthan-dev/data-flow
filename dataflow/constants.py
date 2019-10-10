"""A module that defines project wide constants."""

import os

AUTHBROKER_CLIENT_ID = os.environ.get('AUTHBROKER_CLIENT_ID')
AUTHBROKER_CLIENT_SECRET = os.environ.get('AUTHBROKER_CLIENT_SECRET')
AUTHBROKER_ALLOWED_DOMAINS = os.environ.get('AUTHBROKER_ALLOWED_DOMAINS')
AUTHBROKER_URL = os.environ.get('AUTHBROKER_URL')
FINANCIAL_YEAR_FIRST_MONTH_DAY = os.environ.get('FINANCIAL_YEAR_FIRST_MONTH_DAY')
HAWK_ID = os.environ.get('HAWK_ID')
HAWK_KEY = os.environ.get('HAWK_KEY')
HAWK_ALGORITHM = os.environ.get('HAWK_ALGORITHM')
DATAHUB_BASE_URL = os.environ.get('DATAHUB_BASE_URL')
DEBUG = True if os.environ.get('DEBUG') == 'True' else False
INGEST_TASK_CONCURRENCY = int(os.environ.get('INGEST_TASK_CONCURRENCY', 1))
REDIS_URL = os.environ.get('AIRFLOW__CELERY__BROKER_URL')
