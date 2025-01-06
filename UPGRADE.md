# GraphDB Helm Chart Upgrade Guide

## From 1.x and 2.x to 3.0

### Configurations

Version 3.0 of the Kafka Sink Connector introduces a new configuration property, used in error scenarios - `graphdb.sink.poll.backoff.timeout.ms`.
This property is used to slow down record ingestion in case of downstream congestion - those that are intermittent and in which case data flush has failed and
will be retried at a later point in time. The default value is set to `10000` ms, and can be kept as is.

### Data Migration

Because the connector is stateless, no data is kept on disk where the connector is running. Therefore, upgrading the version of the connector would simply require
a restart of the services and re-configuration of the connector instances.

### Use of Kafka producer

The existing producer runtime, which eases the creation of Kafka records and their subsequent ingestion, has been removed
