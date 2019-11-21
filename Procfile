web: /home/vcap/app/bin/paas-wrapper.sh airflow webserver -p 8080
worker: /home/vcap/app/bin/paas-wrapper.sh airflow worker
scheduler: /home/vcap/app/bin/paas-wrapper.sh airflow scheduler
flower: /home/vcap/app/bin/paas-wrapper.sh airflow flower --basic_auth=$FLOWER_USERNAME:$FLOWER_PASSWORD
