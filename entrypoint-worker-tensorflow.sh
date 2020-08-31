#!/bin/sh

set -e

/home/vcap/app/bin/paas-wrapper.sh airflow worker --queues tensorflow --concurrency 1
