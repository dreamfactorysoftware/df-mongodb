# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
## [0.14.0] - 2017-11-03
- Upgrade swagger to OpenAPI 3.0 specification

## [0.13.0] - 2017-09-18
### Added
- DF-1060 Support for data retrieval (GET) caching and configuration

## [0.12.0] - 2017-08-17
### Changed
- Reworked API doc usage and generation
- Reworked schema interface for database services in order to better control caching
- Set config-based cache prefix
- Update with base class changes

## [0.11.0] - 2017-07-27
- Cleanup service config usage

## [0.10.0] - 2017-06-05
### Changed
- Cleanup - removal of php-utils dependency

## [0.9.0] - 2017-04-21
### Added
- DF-811 Add support for upsert
### Changed
- Use new service config handling for database configuration

## [0.8.0] - 2017-03-03
- Major restructuring to upgrade to Laravel 5.4 and be more dynamically available

## [0.7.1] - 2017-01-28
### Fixed
- Allow strings and integers for IDs on record creation

## [0.7.0] - 2017-01-16
### Changed
- Adhere to refactored df-core, see df-database
- Cleanup schema management issues

## [0.6.0] - 2016-11-17
### Changed
- Columns username and password field type to 'text' in configuration table
- DB base class changes to support field configuration across all database types
- Database create and update table methods to allow for native settings

## [0.5.0] - 2016-10-03
### Added
- DF-826 Extend provisioning to include host, port, etc. to simplify and update configuration to latest models
- New 'count_only' query option returns only the count of filtered records

### Fixed
- DF-834 Utilize the BSON Regex instead of the old MongoRegex to fix 'like' syntax

## [0.4.0] - 2016-08-21
### Changed
- General cleanup from declaration changes in df-core for service doc and providers

### Fixed
- DF-741 Make 'fields' optional for creating table

## [0.3.1] - 2016-07-08
### Changed
- General cleanup from declaration changes in df-core

## [0.3.0] - 2016-05-27
### Changed
- Moved seeding functionality to service provider to adhere to df-core changes

## [0.2.3] - 2016-04-27
### Changed
- Removed requirement for old mongo driver (mongo.so)

## [0.2.2] - 2016-04-22
### Added
- Schema changes to support virtual relationships to SQL DB services

### Changed
- Switched to use jensseger connection classes
- Rework schema to be like SQL DB connections

### Fixed
- Use a service provider 
- Handle quoted identifiers 

## [0.2.1] - 2016-03-08
### Added
- Support for "contains", "starts with", and "ends with" SQL-like filter operators

## [0.2.0] - 2016-01-29
### Added

### Changed
- **MAJOR** Updated code base to use OpenAPI (fka Swagger) Specification 2.0 from 1.2

### Fixed

## [0.1.1] - 2015-12-18
### Changed
- Sync up with changes in df-core for schema classes

## 0.1.0 - 2015-10-24
First official release working with the new [df-core](https://github.com/dreamfactorysoftware/df-core) library.

[Unreleased]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.14.0...HEAD
[0.14.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.13.0...0.14.0
[0.13.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.12.0...0.13.0
[0.12.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.11.0...0.12.0
[0.11.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.10.0...0.11.0
[0.10.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.9.0...0.10.0
[0.9.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.8.0...0.9.0
[0.8.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.7.1...0.8.0
[0.7.1]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.7.0...0.7.1
[0.7.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.6.0...0.7.0
[0.6.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.4.0...0.5.0
[0.4.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.3.1...0.4.0
[0.3.1]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.2.2...0.3.0
[0.2.2]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.1.0...0.1.1
