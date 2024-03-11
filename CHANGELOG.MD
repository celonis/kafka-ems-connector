# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased] - yyyy-mm-dd

## [1.8.3] - 2023-08-30

### Fixed

- Error logging for not successful file upload.
- Committing an offset for heterogeneous topics.

### Changed

- `connect.ems.flattener.enable` is true by default.

## [1.8.2] - 2023-08-25

### Added

- Proper logging when connector is misconfigured for `connect.ems.order.field.name`.

### Fixed

- Kotlin's dependency removed from build jar - caused problems with http library.
- A bug that caused connector to fail in case of schema with `union(null, record)`.

## [1.8.1] - 2023-08-18

### Added

- `connect.ems.sink.put.timeout.ms` - property that is used connector to timeout long-running `put`.

### Fixed

- The blocking way of handling the implementation of `put` Sink interface replaced with non-blocking approach handled by internal supervisor that can fail connector in case of timeout. This prevents connector from getting into `zombie` state.

## [1.8.0] - 2023-07-12

### Changed

- The default sizes of commit policy configuration properties and limits for them, so it plays better with EMS.

## [1.7.2] - 2023-07-03

### Changed

- `connect.ems.parquet.write.flush.records` to `connect.ems.parquet.row.group.size.bytes`.

### Fixed

- The use of http client - now it is a singleton instead of instance per request.

## [1.7.1] - 2023-06-27

### Added

- Brought back an option of having null as values of primary key fields by setting `connect.ems.allow.null.pk`.

### Changed

- `connect.ems.parquet.write.flush.records` now refers to bytes rather than records.

### Fixed

- Enriched the file name of the parquet file, so it has information about offsets, size and number of the rows.

## [1.7.0] - 2023-06-23

### Added

- Option of handling Decimal type by converting it to double: `connector.ems.convert.decimals.to.double`.

### Fixed

- Major bug in Commit Policy that caused a connector to not send the files when it should.

## [1.6.2] - 2023-06-12

### Fixed

- Json chunk flattening bugs.

## [1.6.1] - 2023-05-16

### Added

- Contributor notes and docker-compose.

### Fixed

- A bug that caused connector to hang when there was a schema change.

## [1.6.0] - 2023-05-08

### Added

- XML support.
- Experimental in memory mode that does not use file system to create parquet files before sending them - `connect.ems.inmemfs.enable`.

### Fixed

- Minor bug with Kafka metadata.

## [1.5.0] - 2023-03-14

### Added

- New configuration that when enabled will force connector to attach kafka metadata like offset, partition, kafkaTimestamp to parquet file - `connect.ems.embed.kafka.metadata`.
- Primary keys null validation.

### Fixed

- A bug that caused connector to fail in case of null schemas.

## [1.4.0] - 2023-01-30

### Added

- Flattener - connector now can handle non-flat Kafka messages. Following config was added: `connect.ems.flattener.enable`, `connect.ems.flattener.collections.discard`.
- Flattener has now an option of creating JSON blobs for Kafka topics with more advanced nested structures by using `connect.ems.flattener.jsonblob.chunks`.

## [1.3.4] - 2022-10-03

### Fixed

- Minor technical details about releases.

## [1.3.3] - 2022-09-15

### Added

- Properties to control http client: `connect.ems.pool.max.idle`, `connect.ems.pool.keepalive`, `connect.ems.pool.explicit.close`.

### Changed

- Java version support changed from 8 to 11.

## [1.3.2] - 2022-08-01

### Changed

- Contributor notes on release process.

## [1.3.1] - 2022-06-06

### Changed

- `clientId` fixed to `CelonisKafka2Ems`.

## [1.3.0] - 2022-06-02

### Added

- Support for `connect.ems.order.field.name` (`duplicateRemovalOrderColumn`) - so ingestion with Primary Key has an information on how to deduplicate rows. 

### Changed

- README.
- Licensing.

## [1.2.0] - 2022-05-26

### Added

- Support for proxy.
- Obfuscation.
- README.

### Changed

- General refactor.
- Better test coverage.
- Better parquet file handling.


## [0.9.0] - 2022-01-26

Initial version of Kafka EMS Connector. 