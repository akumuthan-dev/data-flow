#!/bin/sh

export PYTHONPATH=/app:${PYTHONPATH}
export AIRFLOW_HOME=/home/vcap/app/airflow
export DEBUG=False

export AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.deny_all

export AIRFLOW__CORE__DAGS_FOLDER=/home/vcap/app/dataflow/dags
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__LOAD_EXAMPLES=False

export AIRFLOW_CONN_DEFAULT_S3="s3://"$(echo $VCAP_SERVICES | jq -r '.["aws-s3-bucket"][0].credentials | "\(.aws_access_key_id | @uri):\(.aws_secret_access_key | @uri)"')"@S3"

export AIRFLOW__CORE__SECURE_MODE=True
export AIRFLOW__CORE__DONOT_PICKLE=True
export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=False

export AIRFLOW__CORE__LOGGING_CONFIG_CLASS=dataflow.logging_config.LOGGING_CONFIG
export AIRFLOW__CORE__REMOTE_LOGGING=True
export AIRFLOW__CORE__REMOTE_LOG_CONN_ID=DEFAULT_S3
export AIRFLOW__CORE__ENCRYPT_S3_LOGS=True
export AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER="s3://"$(echo $VCAP_SERVICES | jq -r '.["aws-s3-bucket"][0].credentials.bucket_name')"/logs"

export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$(echo $VCAP_SERVICES | jq -r '.postgres[0].credentials.uri')

export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CELERY__BROKER_URL=$(echo $VCAP_SERVICES | jq -r '.redis[0].credentials.uri')
export AIRFLOW__CELERY__RESULT_BACKEND="db+${AIRFLOW__CORE__SQL_ALCHEMY_CONN}"
export AIRFLOW__CELERY__FLOWER_BASIC_AUTH="${FLOWER_USERNAME}:${FLOWER_PASSWORD}"
export AIRFLOW__CELERY__FLOWER_PORT=8080

export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=86400
export AIRFLOW__SCHEDULER__PRINT_STATS_INTERVAL=86400

export AIRFLOW__WEBSERVER__AUTHENTICATE=True
export AIRFLOW__WEBSERVER__AUTH_BACKEND=dataflow.airflow_login
export AIRFLOW__WEBSERVER__COOKIE_SECURE=True
export AIRFLOW__WEBSERVER__COOKIE_SAMESITE=Lax

exec $@
