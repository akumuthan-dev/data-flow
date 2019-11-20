#!/bin/sh

airflow initdb
airflow scheduler &
airflow worker &
airflow webserver --pid /tmp/airflow-webserver.pid -p 8080
