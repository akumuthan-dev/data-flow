#!/bin/sh

set -e

pip3 install -r requirements-tensorflow-worker.txt

/home/vcap/app/bin/paas-wrapper.sh airflow worker --queues tensorflow --concurrency 1
