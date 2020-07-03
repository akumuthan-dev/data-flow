"""A module that defines project wide config."""
import os

ACTIVITY_STREAM_BASE_URL = os.environ.get("ACTIVITY_STREAM_BASE_URL")
ACTIVITY_STREAM_HAWK_CREDENTIALS = {
    "id": os.environ.get("ACTIVITY_STREAM_ID", ""),
    "key": os.environ.get("ACTIVITY_STREAM_SECRET", ""),
    "algorithm": "sha256",
}
ACTIVITY_STREAM_RESULTS_PER_PAGE = os.environ.get(
    "ACTIVITY_STREAM_RESULTS_PER_PAGE", 100
)

AUTHBROKER_CLIENT_ID = os.environ.get("AUTHBROKER_CLIENT_ID")
AUTHBROKER_CLIENT_SECRET = os.environ.get("AUTHBROKER_CLIENT_SECRET")
AUTHBROKER_ALLOWED_DOMAINS = os.environ.get("AUTHBROKER_ALLOWED_DOMAINS")
AUTHBROKER_URL = os.environ.get("AUTHBROKER_URL")

DATAHUB_HAWK_CREDENTIALS = {
    "id": os.environ.get("HAWK_ID"),
    "key": os.environ.get("HAWK_KEY"),
    "algorithm": "sha256",
}
DATAHUB_BASE_URL = os.environ.get("DATAHUB_BASE_URL")
EXPORT_WINS_BASE_URL = os.environ.get("EXPORT_WINS_BASE_URL")

DEBUG = True if os.environ.get("DEBUG") == "True" else False
REDIS_URL = os.environ.get("AIRFLOW__CELERY__BROKER_URL")
COUNTRIES_OF_INTEREST_BASE_URL = os.environ.get(
    "COUNTRIES_OF_INTEREST_BASE_URL", "localhost:5000"
)
DATA_STORE_SERVICE_BASE_URL = os.environ.get(
    "DATA_STORE_SERVICE_BASE_URL", "localhost:5050"
)

S3_IMPORT_DATA_BUCKET = os.environ.get("S3_IMPORT_DATA_BUCKET")
S3_RETENTION_PERIOD_DAYS = os.environ.get("S3_RETENTION_PERIOD_DAYS", 7)

DB_TEMP_TABLE_RETENTION_PERIOD_DAYS = os.environ.get(
    "DB_TEMP_TABLE_RETENTION_PERIOD_DAYS", 3
)

ONS_SPARQL_URL = os.environ.get(
    "ONS_SPARQL_URL",
    "https://production-drafter-ons-alpha.publishmydata.com/v1/sparql/live",
)

MATCHING_SERVICE_BASE_URL = os.environ.get("MATCHING_SERVICE_BASE_URL")
MATCHING_SERVICE_BATCH_SIZE = int(os.environ.get("MATCHING_SERVICE_BATCH_SIZE", 100000))
MATCHING_SERVICE_UPDATE = os.environ.get("MATCHING_SERVICE_UPDATE") == "True"
MATCHING_SERVICE_HAWK_CREDENTIALS = {
    "id": os.environ.get("MATCHING_SERVICE_HAWK_ID"),
    "key": os.environ.get("MATCHING_SERVICE_HAWK_KEY"),
    "algorithm": "sha256",
}

DATA_WORKSPACE_S3_BUCKET = os.environ.get("DATA_WORKSPACE_S3_BUCKET")
DATASETS_DB_NAME = os.environ.get("DATASETS_DB_NAME", "datasets_db")
ALLOW_NULL_DATASET_COLUMNS = os.environ.get("ALLOW_NULL_DATASET_COLUMNS") == "True"

HMRC_UKTRADEINFO_URL = os.environ.get(
    "HMRC_UKTRADEINFO_URL",
    "https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads",
)

MARKET_ACCESS_BASE_URL = os.environ.get("MARKET_ACCESS_BASE_URL")
MARKET_ACCESS_HAWK_CREDENTIALS = {
    "id": os.environ.get("MARKET_ACCESS_HAWK_ID"),
    "key": os.environ.get("MARKET_ACCESS_HAWK_KEY"),
    "algorithm": "sha256",
}

CONSENT_BASE_URL = os.environ.get("CONSENT_BASE_URL")
CONSENT_HAWK_CREDENTIALS = {
    "id": os.environ.get("CONSENT_HAWK_ID"),
    "key": os.environ.get("CONSENT_HAWK_SECRET"),
    "algorithm": "sha256",
}
CONSENT_RESULTS_PER_PAGE = 1000

SLACK_TOKEN = os.environ.get("SLACK_TOKEN")

DATA_WORKSPACE_BASE_URL = os.environ.get("DATA_WORKSPACE_BASE_URL")
DATA_WORKSPACE_HAWK_CREDENTIALS = {
    "id": os.environ.get("DATA_WORKSPACE_HAWK_ID"),
    "key": os.environ.get("DATA_WORKSPACE_HAWK_KEY"),
    "algorithm": "sha256",
}

DNB_AUTH_TOKEN = os.environ.get('DNB_AUTH_TOKEN')
DNB_BASE_URL = os.environ.get('DNB_BASE_URL')

DIT_SHAREPOINT_CREDENTIALS = {
    'site_name': os.environ.get('DIT_SHAREPOINT_SITE_NAME'),
    'tenant_id': os.environ.get('DIT_SHAREPOINT_TENANT_ID'),
    'tenant_domain': os.environ.get('DIT_SHAREPOINT_TENANT_DOMAIN'),
    'client_id': os.environ.get('DIT_SHAREPOINT_CLIENT_ID'),
    'client_secret': os.environ.get('DIT_SHAREPOINT_CLIENT_SECRET'),
}

PEOPLE_FINDER_BASE_URL = os.environ.get('PEOPLE_FINDER_BASE_URL')
PEOPLE_FINDER_PRIVATE_KEY = os.environ.get('PEOPLE_FINDER_PRIVATE_KEY')
