# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [PEP 440](https://www.python.org/dev/peps/pep-0440/)
and uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1]

### Fixed
* Public read permissions were not correctly applied to the data bucket in v0.1.0.

## [0.1.0]

### Changed
* The bucket stack has been separated from the application stack for deployment into a separate account.

## [0.0.5]

### Added
* Automated creation of a log bucket for the data bucket.

## [0.0.4]

### Added
* Automated creation of the S3 data bucket.
* Automated deployment of separate test and production environments.

## [0.0.3]

### Added 
* Support for the API to utilize a custom domain.

## [0.0.2]

### Added
* Initial .gitignore file

## [0.0.1]

### Added
* Initial release of fetcher and api applications
