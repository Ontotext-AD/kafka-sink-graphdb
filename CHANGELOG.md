# Kafka Sink Connector Changelog

## Version 3.2.0

### Highlights

- Several fixes related to connector runtime
- Improved transformation logic
- Improved internal synchronization
- Logging enhancements, especially when running in AWS MSK Cluster

### Fixes

- Fixed issue where internal processor thread would not start on connector restart (stop + start)
- Improved internal synchronization which slightly improves performance and reliability.

### New

- Changed base docker image to `cp-server-connect`
- Added `logger.type` runtime parameter - connectors running in AWS MSK Cluster can use this parameter to setup an MSK-specific logger, which translates all `DEBUG` and `TRACE `messages to `INFO`, if the logger
  level is set to `DEBUG` or `TRACE`, respectively
- Added `log.level.override` runtime parameter to explicitly set the logger level. This is mostly useful when using non-default loggers, i.e. MSK Loggers.

## Version 3.0.1

### Highlights

- TLS communication between Kafka Sink Connector and GraphDB
- Fix retry mechanism for Kafka Sink Connector instances deployed in MSK cluster

### Fixed

- Fixed startup issues when using the Kafka Sink Connector in MSK cluster due to Kafka library incompatibilities

### New

- Added new runtime configuration properties for setting up TLS communication:
	- `graphdb.tls.thumbprint` - The SHA-256 thumbprint of the GraphDB certificate (or any one of the certificate chain) that should be imported and trusted by
	  the connector.
	- `graphdb.tls.hostname.verification.enabled` - Whether hostname verification should be performed against the certificate (or certificate chain). Default
	  set to `true`

## Version 3.0.0


### Highlights

- Update to several component and dependency versions
- Kafka Sink Connector now requires Apache Kafka 3.8.0 and Confluent platform 7.8.0
- Fixes and improvements to synchronization of the sink connector
- Overall code restructure

### Breaking

- The Kafka Sink Connector now requires GraphDB version 10.6 and newer
- The Kafka Sink Connector now requires Apache Kafka 3.8.0 and Confluent Platform 7.8.0

### New

- Added new runtime configuration property for backing off record ingestion in case of downstream congestion - `graphdb.sink.poll.backoff.timeout.ms`
  - The configuration property has a default value of `10000` ms
  - If the connector fails to flush records downstream it can retry flushing later. The amount of time it will wait is the backoff time
- A new record processor instance is started for each configured (unique) connector. Multiple tasks of a single connector will share the same record processor,
optimizing resources

### Updated

- Updated internal component dependencies to address detected vulnerabilities
-

### Fixed
- Fixed a synchronization issue in which two distinct tasks would flush record to a single repository downstream
- Fixed a synchronization issue which would prevent graceful shutdown of the connector and may potentially leave repository connections open
- Fixed core logic issues which could result in data loss

### Improvements

- Improved error handling
- Minimized record loss on failure
- Added setting in `docker-compose.yml` for setting up remote debug on connector instances (for development and troubleshooting purposes).
  These settings are disabled by default
