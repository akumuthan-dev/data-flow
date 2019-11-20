export PYTHONPATH=/app

export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2${DATABASE_URL#postgres}"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql${DATABASE_URL#postgres}"

airflow worker
