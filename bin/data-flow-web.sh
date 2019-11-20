#!/bin/sh

export PYTHONPATH=/app:$PYTHONPATH

export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2${DATABASE_URL#postgres}"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql${DATABASE_URL#postgres}"

# Kill any running airflow processes
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9

airflow initdb
airflow scheduler &
airflow webserver -p 8080 &
airflow flower --basic_auth=$FLOWER_USERNAME:$FLOWER_PASSWORD
