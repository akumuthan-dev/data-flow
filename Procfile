web: /home/vcap/app/bin/paas-wrapper.sh airflow webserver -p 8080
worker: /home/vcap/app/bin/paas-wrapper.sh airflow worker
high-memory-worker: /home/vcap/app/bin/paas-wrapper.sh airflow worker --queues high-memory-usage --concurrency 1
scheduler: /home/vcap/app/bin/paas-wrapper.sh /home/vcap/app/bin/airflow-scheduler.sh
flower: /home/vcap/app/bin/paas-wrapper.sh airflow flower

