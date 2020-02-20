# Data Flow

Data Flow is a custom ETL tool that uses airflow to manage data pipelines between different systems.


## Running it with Docker

- Copy sample.env to .env
- Run docker-compose up --build
- Data-flow will be available on http://localhost:8080


## Deployment to production steps:
Deployment is done via the jenkins "Data Flow" task by promoting a commit through dev, staging and production steps.


## Useful Information

- Airflow runs on UTC timezone by the community to prevent confusion that's why UI values are displayed in UTC timezone.
- Airflow creates a dag run for each completed interval between start date and end date. And it doesn't support scheduling tasks for end of each month. It means when August 1 2019 task is triggered, execution day is August 1 but current day is September 1 2019. Mind this when you filter your views. Please refer bellow code to get last day of execution month in jinja templated fields if you ever need it.
```
date_trunc('month', to_date('{{ macros.datetime.strptime(ds, '%Y-%m-%d') +
	macros.dateutil.relativedelta.relativedelta(months=+1, days=-1) }}', 'YYYY-MM-DD'));
```
- Logs show up after task completed it's the expected behaviour with Airflow remote logging. (S3 is used to centralized logs)
