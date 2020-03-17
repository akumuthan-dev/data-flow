"""A module that defines project wide config."""
import os

ACTIVITY_STREAM_BASE_URL = os.environ.get("ACTIVITY_STREAM_BASE_URL")
ACTIVITY_STREAM_ID = os.environ.get("ACTIVITY_STREAM_ID", "")
ACTIVITY_STREAM_SECRET = os.environ.get("ACTIVITY_STREAM_SECRET", "")
ACTIVITY_STREAM_RESULTS_PER_PAGE = os.environ.get(
    "ACTIVITY_STREAM_RESULTS_PER_PAGE", 100
)

AUTHBROKER_CLIENT_ID = os.environ.get("AUTHBROKER_CLIENT_ID")
AUTHBROKER_CLIENT_SECRET = os.environ.get("AUTHBROKER_CLIENT_SECRET")
AUTHBROKER_ALLOWED_DOMAINS = os.environ.get("AUTHBROKER_ALLOWED_DOMAINS")
AUTHBROKER_URL = os.environ.get("AUTHBROKER_URL")

HAWK_ID = os.environ.get("HAWK_ID")
HAWK_KEY = os.environ.get("HAWK_KEY")
HAWK_ALGORITHM = os.environ.get("HAWK_ALGORITHM", "sha256")
DATAHUB_BASE_URL = os.environ.get("DATAHUB_BASE_URL")
EXPORT_WINS_BASE_URL = os.environ.get("EXPORT_WINS_BASE_URL")

DEBUG = True if os.environ.get("DEBUG") == "True" else False
INGEST_TASK_CONCURRENCY = int(os.environ.get("INGEST_TASK_CONCURRENCY", 1))
REDIS_URL = os.environ.get("AIRFLOW__CELERY__BROKER_URL")
COUNTRIES_OF_INTEREST_BASE_URL = os.environ.get(
    "COUNTRIES_OF_INTEREST_BASE_URL", "localhost:5000"
)
DATA_STORE_SERVICE_BASE_URL = os.environ.get(
    "DATA_STORE_SERVICE_BASE_URL", "localhost:5050"
)

S3_IMPORT_DATA_BUCKET = os.environ.get("S3_IMPORT_DATA_BUCKET")

ONS_SPARQL_URL = os.environ.get(
    "ONS_SPARQL_URL",
    "https://production-drafter-ons-alpha.publishmydata.com/v1/sparql/live",
)

MATCHING_SERVICE_BASE_URL = os.environ.get("MATCHING_SERVICE_BASE_URL")
MATCHING_SERVICE_BATCH_SIZE = int(os.environ.get("MATCHING_SERVICE_BATCH_SIZE", 100000))
MATCHING_SERVICE_UPDATE = os.environ.get("MATCHING_SERVICE_UPDATE") == "True"
MATCHING_SERVICE_HAWK_ID = os.environ.get("MATCHING_SERVICE_HAWK_ID")
MATCHING_SERVICE_HAWK_KEY = os.environ.get("MATCHING_SERVICE_HAWK_KEY")
MATCHING_SERVICE_HAWK_ALGORITHM = os.environ.get(
    "MATCHING_SERVICE_HAWK_ALGORITHM", "sha256"
)

DATA_WORKSPACE_S3_BUCKET = os.environ.get("DATA_WORKSPACE_S3_BUCKET")
DATASETS_DB_NAME = os.environ.get("DATASETS_DB_NAME", "datasets_db")
ALLOW_NULL_DATASET_COLUMNS = os.environ.get("ALLOW_NULL_DATASET_COLUMNS") == "True"

HMRC_UKTRADEINFO_URL = os.environ.get(
    "HMRC_UKTRADEINFO_URL",
    "https://www.uktradeinfo.com/Statistics/Documents/Data%20Downloads",
)
