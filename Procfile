web: /home/vcap/app/bin/paas-wrapper.sh alembic upgrade head && /home/vcap/app/bin/paas-wrapper.sh airflow webserver -p 8080
worker: /home/vcap/app/bin/paas-wrapper.sh airflow worker
tensorflow-worker: ./tensorflow-worker-entrypoint.sh
high-memory-worker: /home/vcap/app/bin/paas-wrapper.sh airflow worker --queues high-memory-usage --concurrency 1
scheduler: /home/vcap/app/bin/paas-wrapper.sh /home/vcap/app/bin/airflow-scheduler.sh
flower: /home/vcap/app/bin/paas-wrapper.sh airflow flower

