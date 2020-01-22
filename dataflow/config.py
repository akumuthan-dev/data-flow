"""A module that defines project wide config."""

import os

ACTIVITY_STREAM_BASE_URL = os.environ.get('ACTIVITY_STREAM_BASE_URL')
ACTIVITY_STREAM_ID = os.environ.get('ACTIVITY_STREAM_ID', '')
ACTIVITY_STREAM_SECRET = os.environ.get('ACTIVITY_STREAM_SECRET', '')
ACTIVITY_STREAM_RESULTS_PER_PAGE = os.environ.get(
    'ACTIVITY_STREAM_RESULTS_PER_PAGE', 100
)

AUTHBROKER_CLIENT_ID = os.environ.get('AUTHBROKER_CLIENT_ID')
AUTHBROKER_CLIENT_SECRET = os.environ.get('AUTHBROKER_CLIENT_SECRET')
AUTHBROKER_ALLOWED_DOMAINS = os.environ.get('AUTHBROKER_ALLOWED_DOMAINS')
AUTHBROKER_URL = os.environ.get('AUTHBROKER_URL')

FINANCIAL_YEAR_FIRST_MONTH_DAY = os.environ.get('FINANCIAL_YEAR_FIRST_MONTH_DAY')

HAWK_ID = os.environ.get('HAWK_ID')
HAWK_KEY = os.environ.get('HAWK_KEY')
HAWK_ALGORITHM = os.environ.get('HAWK_ALGORITHM', 'sha256')
DATAHUB_BASE_URL = os.environ.get('DATAHUB_BASE_URL')
EXPORT_WINS_BASE_URL = os.environ.get('EXPORT_WINS_BASE_URL')

DEBUG = True if os.environ.get('DEBUG') == 'True' else False
INGEST_TASK_CONCURRENCY = int(os.environ.get('INGEST_TASK_CONCURRENCY', 1))
REDIS_URL = os.environ.get('AIRFLOW__CELERY__BROKER_URL')
COUNTRIES_OF_INTEREST_BASE_URL = os.environ.get(
    'COUNTRIES_OF_INTEREST_BASE_URL', 'localhost:5000'
)

S3_IMPORT_DATA_BUCKET = os.environ.get("S3_IMPORT_DATA_BUCKET")

ONS_SPARQL_URL = os.environ.get(
    "ONS_SPARQL_URL",
    "https://production-drafter-ons-alpha.publishmydata.com/v1/sparql/live",
)
