#!/bin/bash

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
			-d '{"id": "test", "params": {"imports": {"name": "imports", "label": "Imported RDF files('\'';'\'' delimited)", "value": ""}, "defaultNS": {"name": "defaultNS", "label": "Default namespaces for imports('\'';'\'' delimited)", "value": ""}}, "title": "", "type": "graphdb", "location": ""}'
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
				"tasks.max":"10",
				"errors.tolerance" : "all",
				"offset.storage.file.filename": "/tmp/storage",
				"graphdb.server.url": "http://graphdb:7200",
				"graphdb.server.repository": "test",
				"graphdb.batch.size": 50,
				"graphdb.batch.commit.limit.ms": 5000,
				"graphdb.auth.type": "NONE",
				"graphdb.update.type": "ADD",
				"graphdb.update.rdf.format": "nq"
			}
		}' http://localhost:8083/connectors -w "\n"
	fi
}

docker compose up -d connect-1 broker zookeeper graphdb

# wait for graphdb and connect to start
wait_service 'http://localhost:7200/protocol'
wait_service 'http://localhost:8083'

# create a test repository in graphdb
create_graphdb_repo
# create the graphdb kafka sink connector
create_kafka_sink_connector

echo "Starting test sequence 1"

for i in $(seq 5); do
	for a in $(seq 25); do
		echo "<urn:test-1> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> ." |  docker container exec -i broker /usr/bin/kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic test &
	done
	wait
done

echo "Test data for test sequence 1 sent. Sleeping for 20 seonds"
sleep 20

echo "Starting test sequence 2, simulating downstream errors (stopping graphdb)"
docker stop graphdb

sleep 10

echo "Sending test data for sequence 2"
for i in $(seq 5); do
	for a in $(seq 25); do
		echo "<urn:test-2> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> ." |  docker container exec -i broker /usr/bin/kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic test &
	done
	wait
done

echo "Test data for test sequence 2 sent."

echo "Starting test sequence 3. Starting graphdb"
docker start graphdb

sleep 10
echo "Sending test data for test sequence 3"
for i in $(seq 5); do
	for a in $(seq 10); do
		echo "<urn:test-3> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> ." |  docker container exec -i broker /usr/bin/kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic test &
		echo "<urn:test-3-invalid <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> ." |  docker container exec -i broker /usr/bin/kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic test &
		echo "<urn:test-3> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> <urn:$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)> ." |  docker container exec -i broker /usr/bin/kafka-console-producer --request-required-acks 1 --broker-list localhost:29092 --topic test &
	done
	wait
done
echo "Test data for test sequence 3 sent. Sleeping for 20 seconds"
sleep 20

docker stop connect-1

echo "Querying ingested data"
echo "Test-1 number of ingested records: $(curl --location 'http://localhost:7200/repositories/test?query=select+*+where+%7B+%3Curn%3Atest-1%3E+%3Fp+%3Fo+.%0A%7D+limit+1000' | grep -c  'urn')"
echo "Test-2 number of ingested records: $(curl --location 'http://localhost:7200/repositories/test?query=select+*+where+%7B+%3Curn%3Atest-2%3E+%3Fp+%3Fo+.%0A%7D+limit+1000' | grep -c  'urn')"
echo "Test-3 number of ingested records: $(curl --location 'http://localhost:7200/repositories/test?query=select+*+where+%7B+%3Curn%3Atest-3%3E+%3Fp+%3Fo+.%0A%7D+limit+1000' | grep -c  'urn')"
echo "Test-3-invalid number of ingested records: $(curl --location 'http://localhost:7200/repositories/test?query=select+*+where+%7B+%3Curn%3Atest-3-invalid%3E+%3Fp+%3Fo+.%0A%7D+limit+1000' | grep -c 'urn')"


docker compose down
