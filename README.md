# Data Flow

Data Flow uses airflow to manage data pipelines between datahub and dataworkspace.
It manages a certain pipeline structure until more pipelines introduced:

- Task 1: Check if target table exists (PostgresOperator)
- Task 2: Get all paginated data from source by making hawk authenticated GET request to given source API's URL. (PythonOperator)
- Task 3: If target table in place (result from Task 1), test validity of incoming data by creating copy table and inserting data there before doing any change on target table. Then, insert fetched data (result from Task 2) into target table

Task 1 and Task 2 are independent and running in parallel.

Task 3 requires on successful run of Task 1 and Task2.

There are currently two generic pipeline structure uses meta objects to dynamically creates DAGs.

They are placed under dataflow/meta folder. Please check regarding docstrings for more information about meta objects.


## How to define dataset pipeline
Add your dataset pipeline to dataflow/meta/datasets_flow.py
(Pipeline name must contain 'DatasetPipeline' to be scheduled)

- Example DatasetFlow
```
class OMISDatasetPipeline:
    # Target table name
    table_name = 'omis_dataset'
    # Source API access url
    source_url = '{}/v4/dataset/omis-dataset'.format(DATAHUB_BASE_URL)
    # Target Database
    target_db = 'datasets_db'
    # Start date for this flow
    start_date = datetime.now().replace(day=1)
    # End date for this flow
    end_date = datetime(2019, 12, 01)
    # Maps source API response fields with target db columns
    # (source_response_field_name, target_table_field_name, target_table_field_constraints)
    field_mapping = [
        (
            'reference',
            'omis_order_reference',
            'character varying(100) PRIMARY KEY'
        ),
        (
            'company__name',
            'company_name',
            'character varying(255) NOT NULL'
        )
        (
            'completed_on',
            'completion_date',
            'timestamp with time zone'
        ),
        (
            'delivery_date',
            'delivery_date',
            'date'
        ),
        (
            'cancellation_reason__name',
            'cancellation_reason',
            'text'
        ),
	...
   ]
``` 

## How to define view pipeline
 Add your view pipeline to dataflow/meta/view_pipelines.py
 Pipeline name must contain 'ViewPipeline' to be scheduled

'where_clause' is jinja2 templated, you can use custom by passing params or use airflow builtin macros
For more info: https://airflow.apache.org/macros.html

```
class CompletedOMISOrderViewPipeline():
    view_name = 'completed_omis_orders'
    dataset_pipeline = OMISDatasetPipeline
    start_date = datetime(2017, 11, 1)
    end_date = datetime(2018, 2, 1)
    catchup = True
    fields = [
        ('company_name', 'Company Name'),
        ('dit_team', 'DIT Team'),
        ('subtotal', 'Subtotal'),
        ('uk_region', 'UK Region'),
        ('market', 'Market'),
        ('sector', 'Sector'),
        ('services', 'Services'),
        ('delivery_date', 'Delivery Date'),
        ('payment_received_date', 'Payment Received Date'),
        ('completion_date', 'Completion Date'),
    ]
    where_clause = """
        order_status = 'complete' AND
        date_trunc('month', completion_date)::DATE =
            date_trunc('month', to_date('{{ yesterday_ds }}', 'YYYY-MM-DD'));
    """
    schedule_interval = '@monthly'
```

## Running it with Docker
- Copy sample.env to .env
- Run docker-compose up --build
- Data-flow will be available on http://localhost:8080

## Running it locally (Automated setup script in the planning)
- Get postgres running on your local machine
- Use init.sql to initialize dbs
- Create virtualenv
- Install pypi dependencies 
- Update local.env for your configurations
- Copy local.env to .env
- run ./entrypoint.sh

## Deployment to production steps:

- Step 1: Set ENV variables (sample.env can be used as a reference, or check below)

- AIRFLOW_CONN_DATASETS_DB = postgresql+psycopg2://'{{ datasets_db_connection_uri }}'
- AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://'{{ airflow_meta_db_connection_uri }}'
- AIRFLOW__CORE__EXECUTOR=LocalExecutor
- AIRFLOW__CORE__DAGS_FOLDER='{{ project_root_directory }}'/dataflow/dags
- AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
- AIRFLOW__CORE__FERNET_KEY='{{ fernet_key_to_secure_db_credentials }}'
```   
from cryptography.fernet import Fernet
fernet_key= Fernet.generate_key()
print(fernet_key.decode()) # your fernet_key, keep it in secured place!
```
- AIRFLOW__CORE__LOAD_EXAMPLES=False
- AIRFLOW_HOME='{{ project_root_directory }}'/airflow
- AIRFLOW__WEBSERVER__AUTHENTICATE=False
- AIRFLOW__WEBSERVER__AUTH_BACKEND=dataflow.airflow_login
- AUTHBROKER_CLIENT_ID='{{ production-staff-sso-client-id }}'  # When it's created redirect url needs to point production {{ data-flow-production-url }}/oauth2callback
- AUTHBROKER_CLIENT_SECRET='{{ production-staff-sso-client-secret }}'
- AUTHBROKER_URL=https://sso.trade.gov.uk/
- AUTHBROKER_ALLOWED_DOMAINS='digitial.trade.gov.uk,trade.gov.uk’
- DATAHUB_BASE_URL='{{ production-data-hub-url }}'
- FINANCIAL_YEAR_FIRST_DAY_MONTH='4-10'
- DEBUG=False
- HAWK_ID='{{ data-flow-hawk-id }}' # Needs to be defined in env var DATA_FLOW_API_ACCESS_KEY_ID in data-hub production
- HAWK_KEY=some-key # Needs to be defined in env var DATA_FLOW_API_SECRET_ACCESS_KEY
- HAWK_ALGORITHM=sha256
- PYTHONPATH='{{ project_root_directory }}':$PYTHONPATH

- Step 2:
Project is configured for buildpack deployment on PaaS, normal process can be followed to deploy on PaaS
- Step 3:
As described in step 1,
	set DATA_FLOW_API_ACCESS_KEY_ID and 
	    DATA_FLOW_API_SECRET_ACCESS_KEY env vars in data-hub production
- Step 4:
Add data-flow ip into HAWK_RECEIVER_IP_WHITELIST env var in data-hub production


## Useful Information
- Airflow runs on UTC timezone by the community to prevent confusion that's why UI values are displayed in UTC timezone.
- Possible schedule_interval values. For more info: (https://airflow.apache.org/scheduler.html)

             '@once' # Schedule once and only once
             '@hourly' # Run once an hour at the beginning of the hour
             '@daily' # Run once a day at midnight CRON: 0 0 * * *
             '@weekly' # Run once a week at midnight on Sunday morning CRON: 0 0 * * 0
             '@monthly' # Run once a month at midnight of the first day of the month CRON: 0 0 1 * *
             '@yearly' # Run once a year at midnight of January 1 CRON: 0 0 1 1 *
- You can find all constants under dataflow/constants.py. Please avoid directly getting env vars from os module instead define it in constants.py file.
- FINANCIAL_YEAR_FIRST_DAY_MONTH can be set as an environment variable. Currently, it's only being used in CancelledOMISOrderViewPipeline. Year of financial year first date is dynamically calculated based on the task's execution date.
- If fields attribute of ViewPipeline is set to True, the view will be created by using regarding dataset fields. Use this when you want to include all fields in dataset without using alias.
