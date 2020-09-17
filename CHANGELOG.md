# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 2020-09-15

### Changed
- Use postgres's native `copy_from` rather than `DataFrame.to_sql` to load data in postgres for the "fast polling" pipelines for significant speed gains (~15x).

## 2020-09-14

### Added

- Contact and advisor last interaction pipelines.

### Changed
- ONS UK trade in services migrated to new polling pipeline.
- ONS UK total trade migrated to new polling pipeline.

## 2020-09-12

### Added

- The ability to send notification emails to users when a pipeline has completed successfully, configured through env vars.

### Changed

- ONS UK SA Trade in Goods dataset to scrape/clean/transform/load directly from the ONS spreadsheet rather than their alpha API, which has been flaky. This will also use our condensed polling pipeline to load this data more quickly.
- ONS UK Trade in goods by country and commodity dataset to a condensed polling pipeline.

## 2020-09-11

### Added

- Allow for pipelines to specify setting `source_data_modified_utc` to run time (utc)

### Changed

- Update daily dataset dags to set `source_data_modified_utc` to utc now

## 2020-09-08

### Changed

- Update queries using legacy `ref_countries_and_territories` reference datasets to use new `ref_countries_territories_and_regions` dataset

## 2020-09-04

### Added

- HMRC Field Force matching pipeline

## 2020-09-03

### Changed

- Fix incorrect column name in People Finder people pipeline

## 2020-09-01

### Added

- 'country_investment_originates_from' to Investment Projects dataset.

## 2020-08-28

### Added

- New investment projects pipeline DataHubMonthlyInvesmentProjectsPipline

## 2020-08-26

### Changed

- Updated fields in People Finder people pipeline

## 2020-08-25

### Added

- 'SSO email' to Advisers dataset.

## 2020-08-24

### Changed

- Add numeric data type to generic dss pipeline

## 2020-08-24

### Changed

- Renamed DirectoryFormsPipeline to GreatGovUKFormsPipeline.

## 2020-08-21

### Changed

- Update maintenance dag to include all (non user and postgres) schemas

## 2020-08-20

### Added

- Directory Forms API pipeline.
- New HMRC pipeline for exports to the EU
- New HMRC pipeline for import from the EU 

### Changed

- Use hmrc schema for all hmrc pipelines

## 2020-08-18

### Added

- Metadata table to the datasets DB under a `dataflow` schema, used to track various information related to pipeline runs/data pulled in. To begin with, we record the last modification date for ONS datasets and the timestamp when dataflow swapped temporary ingest tables with the "real" tables.
- Updated ONS parsing pipelines to only continue if there is more recent data available, based on the source data modified timestamp stored in the metadata table.

## 2020-08-17

### Added

- New pipeline to sync changes from the dnb company service to the dnb global companies table

## 2020-08-13

### Changed

- Move dun and bradstreet tables to dun_and_bradstreet schema
- Rename dun and bradstreet tables to differentiate from global data

## 2020-08-10

### Changed

- Remove Data Hub's obslete field `accepts_dit_email_marketing`, using new field `email_marketing_consent` should be used instead.
- Updated market access trade barriers endpoint and small change to the resulting table structure.

## 2020-08-05

### Added

- A new pipeline task between `insert-into-temp-table` task and `check-temp-table-data` task for transforming data before making public.
- A new field `email_marketing_consent` to Contacts dataset to hold Email consent data joined from Consent dataset.
- Additional transformations for google covid19 mobility data

## 2020-08-03

### Added

- Apple covid19 mobility report pipeline

## 2020-07-28

### Added

- Google covid19 mobility report pipeline

## 2020-07-28

### Added

- Export opps enquiries and companies house matching pipeline

## 2020-07-28

### Changed

- Remove checks from DSSGenericPipeline

## 2020-07-28

### Changed

- `consent_dataset` schema and table name were altered to match our new standards. It will be `dit`.`consent_service__current_consents` now.

## 2020-07-23

### Changed

- Disable `force_http` when making api calls to Consent Service

## 2020-07-22

### Changed

- Allow for granting table access to a list of default db users

## 2020-07-21

### Changed

- New field `commercial_value_explanation` added to market access trade barriers pipeline

## 2020-07-17

### Added

- Joined date to SSO users

## 2020-07-16

### Added

- Generic pipeline to ingest DSS datasets

## 2020-07-16

### Added

- Permitted applications for SSO Users

### Changed

- Add lead adviser details to service delivery csv pipelines

## 2020-07-15

### Added

- Integration tests for api auth backend

## 2020-07-13

### Added

- Protect airflow api with custom hawk auth backend

## 2020-07-10

### Added

- Last access time for SSO Users

## 2020-07-09

### Changed

- Updates to normalised fields across ons datasets
    - Normalise totals fields to int as all values are rounded to millions
    - Replace empty strings with nulls on marker fields
    - Replace 'N/A' and 'not-applicable' with 'not-available' on marker fields

## 2020-07-08

### Added

- Remove public information asset register DAG
- New data set for Staff SSO users

### Changed

- Update fdi dashboard pipeline to take into account stage and status of projects

## 2020-07-03

### Added

- New data set for companies house company data

## 2020-07-02

### Added

- New data hub dataset for company referrals

### Changed

- Install wheel before requirements file to fix circle ci build issue
- Remove Alexey from depndabot config

## 2020-07-01

### Changed

- Changed People Finder pipeline to support new/updated fields

## 2020-07-01

### Changed

- Changed People Finder pipeline to support new/updated fields

## 2020-06-30

### Removed

- Removed People Finder CSV pipeline (this will be handled as data cuts in Data Workspace instead)

## 2020-06-30

### Changed

- Update sharepoint iar pipelines to take into account prod site layout

## 2020-06-29

### Changed

- Remove field `dumping_margin_applies` from global tariff dataset
- New field `cet_applies_until_trade_remedy_transition_reviews_concluded` added to tariff dataset

## 2020-06-26

### Changed

- Force http requests for consent service to fix hawk auth issue

## 2020-06-23

### Changed

- S3 maintenance job will always retain data files from the latest run of each pipeline.
- ONSUKTradeInServicesByPartnerCountryNSAPipeline from daily schedule to monthly.

## 2020-06-22

### Changed

- Table name for ONS dataset 3 to include double-underscore separate between the data source (ONS) and the data description.
- Update ONS dataset 2+4 to include double-underscore separator, and change column names to match our data standards document.

## 2020-06-18

### Added

- Ability to fetch data from client-generated JWT authenticated API
- DIT People Finder ("Digital Workspace") people data

### Changed

- Updated ONS dataset 3 master dataset to follow our data standards.

## 2020-06-12

### Changed

- Fixed bug where incorrect schema was used to grant permissions after table swap

## 2020-06-11

### Added

- COVID-19 Johns Hopkins University global time series data

## 2020-06-10

### Changed

- Made auth token optional when fetching from external api.

### Added

- New dataset for global uk tariff data 
- New sharepoint integration and 2 sharepoint datasets
  - Information Asset Register
  - Public Information Asset Register
- Added `archived_reason` and `created_by_id` fields to companies dataset
- Added `last_login` field to advisers dataset
- Added `created_by_id` field to contacts dataset
- Added `created_by_id` field to omis dataset
- Added `created_by_id` and `disabled_on` fields to events dataset
- Added `disabled_on` field to teams dataset

## 2020-06-09

### Changed

- Added the column 'were_countries_discussed' to the Interactions dataset.

## 2020-06-08

### Changed

- ONS dataset 2 query based on feedback from data team.

## 2020-06-05

### Changed

- Use financial year rather than tax year on export wins financial year csv 
- Fix typos in oxford covid19 dataset
- ONS datasets 1,3,4 queries based on feedback from data team.

## 2020-06-03

### Added

- Added Oxford covid-19 government response tracker pipeline 

## 2020-06-01

### Added

- Product name to ONS dataset 2.

## 2020-05-29

### Added

- CSV Pipeline for ONS dataset 2 "uktradecountrybycommodity"
- Yearly CSV pipeline reporting on export wins by financial year

### Changed

- Schedule for ONS dataset 2 pipelines - ONS refreshes source data monthly around the 13th.

## 2020-05-26

### Added

- Pipeline for ONS dataset 2 "uktradecountrybycommodity"

## 2020-05-22

### Changed

- Optionally allow for use of a db schema other than public

## 2020-05-21

### Changed

- Start export wins yearly reports from 2016

## 2020-05-19

## Added

- ONS dataset 4 "ONSUKTotalTradeAllCountriesNSA"

### Changed

- Added trade balance/total trade to `ONSUKTotalTradeAllCountriesNSA` CSV export.

### Removed

- Old pipeline 'ONSUKTotalTradeInServicesByPartnerCountry' using gss-data (we've changed to scraping XLSX ourselves for now).

## 2020-05-17

## Added

- ONS dataset `ONSUKTradeInServicesByPartnerCountryNSA`, plus CSV pipeline.

## 2020-05-14

### Changed

- Fix typo in ministerial interactions column name
- Remove unnecessary casting of all fields to text on the fdi dashboard pipeline 
- Fix bug where TypeError was not being raised correctly on JsonEncoder
- JSON encode decimals when writing to s3

## 2020-05-13

### Changed

- Use underscored, lower case column names for ministerial interactions dashboard

## 2020-05-11

### Changed

- Set primary key to auto incrementing integer for ministerial interactions dashboard

## 2020-05-07

### Changed

- Automatically cast dates/times to strings when json encoding for an s3 write.
- Add a primary key to ministerial interactions dashboard dataset

## 2020-05-06

- Move dashboard DAGs to a single file `dashboard_pipelines.py`
- Add new dashboard DAG for ministerial interactions 

## 2020-05-05

### Changed

- Remove primary key from consent service pipeline
- Timeout hawk API requests after 5 minutes, so that retries can happen.
- Enable run-fetch full task retries on the Interactions pipeline.

## 2020-05-04

### Added

- New pipeline for consumption of dnb data via the dnb-service

## 2020-04-29

### Changed

- Bump page_size for interactions dataset in order to (hopefully) reduce run time.
- Set max time for requests to try to error earlier if things get stuck.

## 2020-04-22

### Changed

- Ensure rejected export wins are not included in yearly report

### Removed

- World bank tariff (data-store-service) pipeline
- DIT BACI (data-store-service) pipeline

## 2020-04-21

### Changed

- Add a workaround for http/https hawk auth issues on data workspace
- Catch type errors when passing records and the nested field is None

## 2020-04-15

### Added

- 3 new data workspace datasets
  - Users
  - Events
  - Application instances

## 2020-04-14

### Changed

- Use the dag run end date when refreshing CSV files

## 2020-04-09

### Changed

- Updated the Market Access Trade Barriers to no longer read the `resolved_date` field, which was removed in the upstream API.
- Updated the Interactions pipeline to include a page_size=1000 query param (the default is 100), so that this pipeline hopefully completes sooner.
- Set the timestamp on CSVs to the date that the file is created (rather than the start date of the task)
- Bring CSV output file names in line with the agreed naming conventions

## 2020-04-08

### Added

- Maintenance DAG with tasks to clean up old S3 files and temporary DB tables
- CSV pipeline for ONS UK Trade in Services
- CSV pipeline for ONS UK Total Trade in Services

### Changed

- ONS UK Trade in services pipelines now include period types, and shouldn't have duplicated data for quarterly rows.
- Set schedule to yearly for DIT BACI pipeline
- Set schedule to yearly for world bank tariff pipelines
- Updated duty type field type to text on raw world bank tariff pipeline
- Added columns eu_rep_rate and eu_part_rate to world bank tariff pipeline
- Updated partner and reporter field types to text on world bank tariff pipeline
- S3Data methods are now retried in case of temporary network failure

## 2020-04-07

### Added

-  DIT BACI (data-store-service) pipeline

## 2020-04-06

### Changed

- Turn off catchup for daily csv dags

## 2020-04-03

### Added

- Support for dependencies for the CSV pipelines
- ONSUKSATradeInGoodsCSV pipeline

### Changed

- Clearer error when insert task fails due to no data in S3.

## 2020-03-30

### Added

- Timeouts on data pipeline tasks to prevent them from getting stuck waiting for DB lock
- Slack notifications for failed/successful DAG runs
- Pipeline DAG base class now supports cross-DAG dependencies using external task sensors
- airflow upgradedb now runs on each deployment

### Changed

- New data pipeline DAG structure with separate clean-up steps for temp and swap tables
- Increased log levels to reduce airflow log noise
- Removed canary task

## 2020-03-26

### Added

- Consent API pipeline.
- Raw world bank tariff pipeline.
- Raw world bank bound rates pipeline.

## 2020-03-25

### Changed

- Migrate more pipelines to using `TableConfig` (dataset_pipelines.py)

## 2020-03-19

### Added

- Market access trade barriers pipeline.

### Changed

- Moved datasets `fetch_from_api` to a more general `common.fetch_from_hawk_api`, as there may be a more general case for this.

## 2020-03-12

### Changed

- Fix operator log messages not being included in task S3 logs

## 2020-03-11

### Added

- `TableConfig` as the canonical way to specify how data is pulled into SQL tables. This includes the ability to handle nested (and deeply nested) related resources.
- A pipeline for Data Hub Service Performance Indicator (SPI) Investment Reports.

### Changed

- The existing `AdvisorDatasetPipeline` to use the new `TableConfig` process.

## 2020-03-03

### Changed

- Updated World bank tariff pipeline
  - removed prf_rate
  - added indexes to partner, reporter, product and year columns

## 2020-03-03

### Added

- World bank tariff (data-store-service) pipeline

## 2020-02-27

### Changed

- Add created by id to interactions dataset

## 2020-02-26

### Changed

- Update monthly static FDI report query
  - Join string arrays with semicolons
  - Do not include empty string for `other_business_activities` field
- Ensure omis orders are included to the end of the month for the given run date
- Set null and false values to false in export wins early cut

## 2020-02-25

### Changed

- Set monthly static fdi report window to end of the current month

## 2020-02-24

### Changed

- Mark monthly fdi pipeline as static

## 2020-02-20

### Changed

- Make column validation configurable per pipeline

## 2020-02-20

### Added

- 5 new daily csv pipelines
    - DataHubServiceDeliveriesCurrentYearDailyCSVPipeline
    - DataHubInteractionsCurrentYearDailyCSVPipeline
    - DataHubServiceDeliveriesPreviousYearDailyCSVPipeline
    - DataHubInteractionsPreviousYearDailyCSVPipeline
    - ExportWinsCurrentFinancialYearDailyCSVPipeline

### Changed

- Order cancelled omis orders by cancelled date
- Include all fdi records up to current actual run time in daily report

## 2020-02-13

### Added

- Pipeline for matching datasets using the CompanyMatching service

## 2020-02-18

### Added

- CSV pipelines added to replace view pipelines
  - Reports are output directly to s3 to be made downloadable via data workspace
  - Split pipelines out into daily, monthly, yearly config files
  - Adds a new daily pipeline that recreates all historic non-static csv files

### Changed

- Dataset pipelines configuration brought inline with new pipeline setup
  - Use sqlalchemy to configure tables
  - Single 'insert to db' process
  - Fetched pages stored in s3 to be picked up by insert task
  - Users are granted to access to newly created tables before they are removed

### Removed

- As part of the above changes all view pipelines have been removed

## 2020-02-11

### Added

- Two new ONS datasets for UK trade in services.
- ONS Postcode (data-store-service) pipeline

## 2020-02-04

### Changed

- Fixed bug where business activity was not string formatted

## 2020-01-23

### Changed

- Added 2 new fields to the company dataset
  - `global_headquarters_id`
  - `global_ultimate_duns_number`
- Renamed company dataset field `classification` to `one_list_tier`
- Added 1 new field to the interaction dataset
  - `modified_on`
