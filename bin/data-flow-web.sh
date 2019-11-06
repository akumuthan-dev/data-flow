#!/bin/sh

export PYTHONPATH=/app:$PYTHONPATH

# Kill any running airflow processes
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9

airflow initdb
airflow scheduler &
airflow webserver -p 8080 &
airflow flower --basic_auth=$FLOWER_USERNAME:$FLOWER_PASSWORD
