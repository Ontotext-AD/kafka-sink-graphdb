# Kafka Sink Connector Changelog

## Version 3.0.0


### Highlights

- Update to several component and dependency versions
- Kafka Sink Connector now requires Apache Kafka 3.8.0 and Confluent platform 7.8.0
- Fixes and improvements to synchronization of the sink connector
- Overall code restructure

### Breaking

- The Kafka Sink Connector now requires Apache Kafka 3.8.0 and Confluent Platform 7.8.0

### New

- Added new runtime configuration property for backing off record ingestion in case of downstream congestion - `graphdb.sink.poll.backoff.timeout.ms`
  - The configuration property has a default value of `10000` ms
  - If the connector fails to flush records downstream it can retry flushing later. The amount of time it will wait is the backoff time
- A new record processor instance is started for each configured (unique) connector. Multiple tasks of a single connector will share the same record processor,
optimizing resources

### Updated

- Updated internal component dependencies to address detected vulnerabilities

### Fixed
- Fixed a synchronization issue in which two distinct tasks would flush record to a single repository downstream
- Fixed a synchronization issue which would prevent graceful shutdown of the connector and may potentially leave repository connections open
- Fixed core logic issues which could result in data loss

### Improvements

- Improved error handling
- Minimized record loss on failure
- Added setting in `docker-compose.yml` for setting up remote debug on connector instances (for development and troubleshooting purposes).
These settings are disablbed by default
