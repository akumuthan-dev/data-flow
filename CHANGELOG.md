# Changelog
  
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## 2020-02-27

### Changed

- Add created by id to interactions dataset


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
