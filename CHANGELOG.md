# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added

### Changed

### Fixed

## [0.2.3] - 2016-04-27
### Changed
- Removed requirement for old mongo driver (mongo.so)

## [0.2.2] - 2016-04-22
### Added
- Schema changes to support virtual relationships to SQL DB services

### Changed
- Switched to use jensseger connection classes.
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

[Unreleased]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.2.2...HEAD
[0.2.2]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/dreamfactorysoftware/df-mongodb/compare/0.1.0...0.1.1
