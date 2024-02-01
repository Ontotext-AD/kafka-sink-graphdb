# kafka-sink-graphdb
Kafka Sink Connector for RDF update streaming to GraphDB

The current version of Apache Kafka in project is 3.5.x.

Apache Kafka 3.5.x is compatible with Confluent Platform version 7.5.x.

This means you can leverage the latest enhancements in both Apache Kafka and Confluent Platform to enhance the performance,

scalability, and features of your streaming applications.

## Upgrading Kafka Version

When upgrading your Apache Kafka installation, it's crucial to ensure compatibility between Confluent and Apache Kafka versions.

Follow the guidelines provided by the Confluent documentation to guarantee a smooth upgrade process.

Ensure that you refer to the compatibility matrix to find the suitable Confluent Platform version for your desired Apache Kafka version. The matrix can be found at the following link:

[Confluent Platform and Apache Kafka Compatibility Matrix](https://docs.confluent.io/platform/current/installation/versions-interoperability.html#cp-and-apache-ak-compatibility)

Follow these steps to upgrade your Kafka installation:

1. Review the compatibility matrix to identify the Confluent Platform version that matches your target Apache Kafka version.
2. Follow the upgrade instructions provided by Confluent for the chosen Confluent Platform version.

It's recommended to perform thorough testing in a staging environment before applying the upgrade to your production environment.

For detailed instructions and additional information, consult the official Confluent documentation.

# Docker & Docker Compose

A [Dockerfile](./Dockerfile) is available for building the sink connector as a docker image.
It is a multistage dockerfile which builds, tests and in the final stage copies the connector on the `plugin.path`.

The image is based on Confluent Kafka Connect [confluentinc/cp-kafka-connect](https://hub.docker.com/r/confluentinc/cp-kafka-connect) image.

To build the image navigate to the project root directory and execute `docker build -t kafka-sink-graphdb .`

Inside the [docker-compose](./docker-compose) directory there is an example compose file that sets everything up - ZooKeeper, Kafka, GraphDB and Kafka Connect.
In the same directory the [run.sh](./docker-compose/run.sh) script can be used to quickly test the sink connector.

The script will do the following:

1. execute `docker-compose up -d` to start the required services
    * if the connector image is not present locally it will be build
1. wait for GraphDB and kafka connect to start
2. create a GraphDB repository named "test"
3. create the kafka-sink-graphdb connector
4. using the Kafka console producer - produce a simple triple (<urn:a> <urn:b> <urn:c> ) to the test topic

If all goes well you should see the triple in GraphDB:
1. open http://localhost:7200/sparql
    * check in the upper right corner that the __test__ repository is selected
2. run the following query
```sparql
select * where {
    <urn:a> ?p ?o  .
}
```

# Enable Security for Failed Messages sent to DLQ

This guide offers step-by-step instructions for initiating a secure Kafka Sink configuration, incorporating security properties.

The configuration details can be specified using properties prefixed with "producer.override." or through environment variables in the Dockerfile.

## Configuration Options

### Utilizing ["producer.override."](https://docs.confluent.io/platform/current/connect/references/allconfigs.html#override-the-worker-configuration) Prefix in Property Configuration

To apply security properties, integrate the required settings into given .properties file.

Provide the "producer.override." prefix to indicate security-related parameters. For example:

```properties

name=GraphDBSinkConnector
connector.class=com.ontotext.kafka.GraphDBSinkConnector

graphdb.server.url=Type: String;\nDescription: GraphDB Server URL
graphdb.server.repository=Type: String;\nDescription: GraphDB Repository

graphdb.batch.size=Type: Int;

graphdb.auth.type=Type: enum[NONE, BASIC, CUSTOM];\nDescription: Authentication type (default NONE)
graphdb.auth.basic.username=Type: String;
graphdb.auth.basic.password=Type: String;
graphdb.auth.header.token=Type: String;\nDescription: GraphDB Auth Token

graphdb.update.type=Type: enum[ADD, REPLACE_GRAPH, SMART_UPDATE];\nDescription: Transaction type (default NONE)
graphdb.update.rdf.format=Type: enum[rdf, rdfs, xml, owl, nt, ttl, ttls, n3, xml, trix, trig, trigs, brf, nq, jsonld, ndjsonld, jsonl, ndjson, rj, xhtml, html, hdt];\nDescription: Transaction type (default NONE)
graphdb.batch.commit.limit.ms=Type: Long;\nDescription: The timeout applied per batch that is not full before it is committed (default 3000)

errors.tolerance=all
#The topic name in kafka brokers to store failed records
errors.deadletterqueue.topic.name=failed-messages-01
#Type:Long;\nDescription: Maximum time in ms to retry sending records
producer.override.errors.retry.timeout=5000
#Type:Long;\nDescription: The time in ms in-between retries
producer.override.errors.retry.delay.max.ms=1000
#Type:List;\nDescription: The IRIs of kafka brokers (default localhost:9092,localhost:9093,localhost:9093)
bootstrap.servers=localhost:9092
producer.override.errors.deadletterqueue.topic.replication.factor=1
producer.override.bootstrap.servers=your-bootstrap-servers
producer.override.security.protocol=SASL_SSL
producer.override.sasl.mechanism=PLAIN
producer.override.sasl.jaas.config=your-sasl-config
```

or pass properties runtime in config on start of the connector

```shell
	if ! curl -s --fail host:port/connectors/kafka-sink-graphdb &> /dev/null; then
		curl -H 'Content-Type: application/json' --data '
		{
			"name": "kafka-sink-graphdb",
			"config": {
				"connector.class":"com.ontotext.kafka.GraphDBSinkConnector",
				"key.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter.schemas.enable": "false",
				"topics":"test",
				"tasks.max":"1",
				"offset.storage.file.filename": "/tmp/storage",
				"graphdb.server.url": "http://graphdb:7200",
				"graphdb.server.repository": "test",
				"graphdb.batch.size": 64,
				"graphdb.batch.commit.limit.ms": 1000,
				"graphdb.auth.type": "NONE",
				"graphdb.update.type": "ADD",
				"graphdb.update.rdf.format": "nq",
				"errors.tolerance": "all",
				"bootstrap.servers": "localhost:9092",
				"errors.deadletterqueue.topic.name": "failed-messages-01",
				"producer.override.errors.retry.timeout": "5000",
				"producer.override.errors.retry.delay.max.ms": "1000",
				"producer.override.errors.deadletterqueue.topic.replication.factor": "1",
				"producer.override.bootstrap.servers": "your-bootstrap-servers",
				"producer.override.security.protocol": "SASL_SSL",
				"producer.override.sasl.mechanism": "PLAIN",
				"producer.override.sasl.jaas.config": "your-sasl-config"
			}
		}' http://host:port/connectors -w "\n"
	fi
```

### Using Environment Variables in Dockerfile

To configure the inner FailedRecordProducer with security properties using environment variables in the Dockerfile:

```dockerfile
version: '3'

services:
  connect-1:
    image: docker-registry.ontotext.com/kafka-sink-graphdb:<your_version>
    hostname: connect-1
    container_name: connect-1
    network_mode: host
    ports:
      - 8083:8083
	environment:
		  CONNECT_BOOTSTRAP_SERVERS: SSL://your_host:your_port
		  CONNECT_REST_ADVERTISED_HOST_NAME: your_host
		  CONNECT_REST_PORT: port
		  CONNECT_REST_HOST_NAME: host
		  CONNECT_GROUP_ID: compose-connect-group-id
		  CONNECT_KEY_CONVERTER: key_serializer.class
		  CONNECT_VALUE_CONVERTER: value_serializer.class
		  CONNECT_INTERNAL_KEY_CONVERTER: key_converter.class
		  CONNECT_INTERNAL_VALUE_CONVERTER: value_converter.class
		  CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs-topic
		  CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
		  CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
		  CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets-storage-topic
		  CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
		  CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status-storage-topic
		  CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
		  CONNECT_ZOOKEEPER_CONNECT: example.test.com:2181

		  CONNECT_SASL_MECHANISM: PLAIN
		  CONNECT_CONSUMER_SASL_MECHANISM: PLAIN
		  CONNECT_PRODUCER_SASL_MECHANISM: PLAIN
		  CONNECT_SASL_JAAS_CONFIG: |
			org.apache.kafka.common.security.plain.PlainLoginModule required \
			username="username" \
			serviceName="servicename" \
			password="password";
		  CONNECT_CONSUMER_SASL_JAAS_CONFIG: |
			org.apache.kafka.common.security.plain.PlainLoginModule required \
			username="username" \
			serviceName="servicename" \
			password="password";
		  CONNECT_PRODUCER_SASL_JAAS_CONFIG: |
			org.apache.kafka.common.security.plain.PlainLoginModule required \
			serviceName="servicename" \
			username="username" \
			password="password";
		  CONNECT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: HTTPS

		  CONNECT_SECURITY_PROTOCOL: SASL_SSL
		  CONNECT_SSL_TRUSTSTORE_FILENAME: /path_to/keystore.jks
		  CONNECT_SSL_TRUSTSTORE_LOCATION: /path_to/keystore.jks
		  CONNECT_SSL_TRUSTSTORE_PASSWORD: changeit
		  CONNECT_SSL_KEYSTORE_FILENAME: /path_to/keystore.jks
		  CONNECT_SSL_KEYSTORE_LOCATION: /path_to/keystore.jks
		  CONNECT_SSL_KEYSTORE_PASSWORD: changeit
		  CONNECT_SSL_KEY_PASSWORD: my_password

		  CONNECT_CONSUMER_SECURITY_PROTOCOL: SASL_SSL
		  CONNECT_CONSUMER_SSL_TRUSTSTORE_LOCATION: /path_to/keystore.jks
		  CONNECT_CONSUMER_SSL_TRUSTSTORE_PASSWORD: changeit
		  CONNECT_CONSUMER_SSL_KEYSTORE_LOCATION: /path_to/keystore.jks
		  CONNECT_CONSUMER_SSL_KEYSTORE_FILENAME: /path_to/keystore.jks
		  CONNECT_CONSUMER_SSL_KEYSTORE_PASSWORD: changeit
		  CONNECT_CONSUMER_SSL_KEY_PASSWORD: my_password

		  CONNECT_PRODUCER_SECURITY_PROTOCOL: SASL_SSL
		  CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION: /path_to/keystore.jks
		  CONNECT_PRODUCER_SSL_TRUSTSTORE_PASSWORD: changeit
		  CONNECT_PRODUCER_SSL_KEYSTORE_LOCATION: /path_to/keystore.jks
		  CONNECT_PRODUCER_SSL_KEYSTORE_FILENAME: /path_to/keystore.jks
		  CONNECT_PRODUCER_SSL_KEYSTORE_PASSWORD: changeit
		  CONNECT_PRODUCER_SSL_KEY_PASSWORD: my_password

		  CONNECT_PRODUCER_SSL_KEYSTORE_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_PRODUCER_SSL_KEY_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_PRODUCER_SSL_TRUSTSTORE_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_PRODUCER_SSL_TRUST_CREDENTIALS: /path_to/kafka-cred.properties

		  CONNECT_CONSUMER_SSL_KEYSTORE_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_CONSUMER_SSL_KEY_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_CONSUMER_SSL_TRUSTSTORE_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_CONSUMER_SSL_TRUST_CREDENTIALS: /path_to/kafka-cred.properties
		  CONNECT_CONSUMER_SSL_CLIENT_AUTH: required
```
> **Note:** </br>
> The DLQ (Dead Letter Queue) producer will initiate only if both "errors.tolerance": "all" and a value for "errors.deadletterqueue.topic.name"</br>
> are either specified in the properties file or passed as runtime properties.</br>
> They cannot be provided as environment variables. </br>
> If security configuration is not found in the properties file or as a runtime property the application </br>
> will provide all resolved environment properties when the kafka producer is created
