# Data Flow

Data Flow is a custom ETL tool that uses airflow to manage data pipelines between different systems.


## Pipelines

There are currently 2 main types of pipelines in Data Flow:

* Data import pipelines fetch data from an external source (like a Data Hub or ONS API) and load them into a new dataset DB table.
* CSV pipelines query the dataset DB tables and generate a CSV file containing a view / cut of the data.

### Data import pipelines

As a result of a data import pipeline run a table in the datasets DB containing the source data is created (or replaced).
Data import pipelines should subclass the base `dataflow.dags._PipelineDAG` class and override `get_fetch_operator` method to return a fetch operator for a given data source.

All data import pipelines currently follow the same process:

1. Create a unique temporary DB table for the current task run.
2. Fetch data from the source and store responses as JSON lists in S3 bucket, prefixed with a unique string for each task run. This task is unique for each data source (e.g. Data Hub, ONS, Activity Stream), but should use the common `dataflow.utils.S3Data` class to make sure the stored responses can be accessed by the following DB tasks.
3. Read response files from S3 and import them into the temporary DB table.
4. Run checks on the new dataset. Currently this includes things like making sure the new dataset isn't significantly smaller than the previous version and that all DB columns are used.
5. Replace the current `.table_name` dataset table and the new table containing latest data.
6. Clean up any temporary / previous dataset tables at the end of the run.

### CSV pipelines

CSV pipelines run a specified query on existing dataset DB tables and save the query results in a CSV file stored in S3. The CSV files can then be downloaded through the Data Workspace.
CSV pipelines should subclass the base `dataflow.dags._CSVPipelineDAG` class and set the required values for `base_file_name` and `query` attributes.


## Running it with Docker

1. Copy sample.env to .env
2. Run docker-compose up --build
3. Data-flow will be available on http://localhost:8080


## Deployment to production

Deployment is done via the jenkins "Data Flow" task by promoting a commit through dev, staging and production steps.


## Useful Information

- Airflow runs on UTC timezone by the community to prevent confusion that's why UI values are displayed in UTC timezone.
- Airflow creates a dag run for each completed interval between start date and end date. And it doesn't support scheduling tasks for end of each month. It means when August 1 2019 task is triggered, execution day is August 1 but current day is September 1 2019. Mind this when you filter your views. Please refer bellow code to get last day of execution month in jinja templated fields if you ever need it.
```
date_trunc('month', to_date('{{ macros.datetime.strptime(ds, '%Y-%m-%d') +
	macros.dateutil.relativedelta.relativedelta(months=+1, days=-1) }}', 'YYYY-MM-DD'));
```
- Logs show up after task completed it's the expected behaviour with Airflow remote logging. (S3 is used to centralized logs)
