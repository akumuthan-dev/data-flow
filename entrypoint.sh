#!/bin/sh

airflow initdb
airflow scheduler &
airflow worker &
airflow webserver -p 8080 
