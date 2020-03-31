#!/bin/sh

airflow upgradedb

airflow pool -s sensors ${AIRFLOW_SENSORS_POOL_SLOTS:-16} "External task sensors"

airflow scheduler
