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
