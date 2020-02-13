# Changelog
  
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 2020-02-13

### Added

- Pipeline for matching datasets using the CompanyMatching service

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
