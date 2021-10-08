#!/usr/bin/env bash

set -eu

function wait_service {
	printf "waiting for $1"
	until curl -s --fail -m 1 "$1" &> /dev/null; do
		sleep 1
		printf '.'
	done
	echo
}

function create_graphdb_repo {
	if ! curl --fail -X GET --header 'Accept: application/json' http://localhost:7200/rest/repositories/test &> /dev/null; then
		curl 'http://localhost:7200/rest/repositories' \
			-H 'Accept: application/json, text/plain, */*' \
			-H 'Content-Type: application/json;charset=UTF-8' \
			-d '{"id":"test", "type":"free", "title": "Kafka connect sink test repository", "params": {}}'
	fi
}

function create_kafka_sink_connector {
	if ! curl -s --fail localhost:8083/connectors/kafka-sink-graphdb &> /dev/null; then
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
				"graphdb.update.rdf.format": "nq"
			}
		}' http://localhost:8083/connectors -w "\n"
	fi
}

docker-compose up -d

# wait for graphdb and connect to start
wait_service 'http://localhost:7200/protocol'
wait_service 'http://localhost:8083'

# create a test repository in graphdb
create_graphdb_repo
# create the graphdb kafka sink connector
create_kafka_sink_connector

# test
echo '<urn:a> <urn:b> <urn:c> .' |  docker container exec -i broker /usr/bin/kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic test

# go to http://localhost:7200/sparql select the test repository and execute select * where { <urn:a> ?p ?o  . }
