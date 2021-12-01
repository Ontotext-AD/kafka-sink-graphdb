# kafka-sink-graphdb
Kafka Sink Connector for RDF update streaming to GraphDB

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
