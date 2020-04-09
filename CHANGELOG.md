# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 2020-04-09

### Changed

- Updated the Market Access Trade Barriers to no longer read the `resolved_date` field, which was removed in the upstream API.

## 2020-04-08

### Added

- Maintenance DAG with tasks to clean up old S3 files and temporary DB tables

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
