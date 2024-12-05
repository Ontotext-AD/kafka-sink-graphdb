#!/usr/bin/env bash

function wait_service() {
	printf "waiting for %s " "$1"
	until curl -s --fail -m 1 "$1" &> /dev/null; do
		sleep 1
		printf '.'
	done
	echo
}

function create_graphdb_repo() {
	REPOSITORY=${1:-test}
	echo "Creating GraphDB repository ${REPOSITORY}"
	if ! curl --fail -X GET --header 'Accept: application/json' "http://localhost:7200/rest/repositories/${REPOSITORY}" &> /dev/null; then
		curl --fail 'http://localhost:7200/rest/repositories' \
			-H 'Accept: application/json, text/plain, */*' \
			-H 'Content-Type: application/json;charset=UTF-8' \
			-d '{"id": "'"${REPOSITORY}"'", "params": {"imports": {"name": "imports", "label": "Imported RDF files('\'';'\'' delimited)", "value": ""}, "defaultNS": {"name": "defaultNS", "label": "Default namespaces for imports('\'';'\'' delimited)", "value": ""}}, "title": "", "type": "graphdb", "location": ""}'
	fi
	if ! curl --fail -X GET --header 'Accept: application/json' "http://localhost:7200/rest/repositories/${REPOSITORY}" &> /dev/null; then
		echo "Could not create repository"
		exit 1
	fi
	echo "Repository ${REPOSITORY} created"
}


function create_kafka_sink_connector() {
	SINK_NAME=${1:-kafka-test-sink}
	TOPIC_NAME=${2:-test}
	MAX_TASKS=${3:-1}
	TOLERANCE=${4:-all}
	GRAPHDB_REPO=${5:-test}
	BATCH_SIZE=${6:-10}
	POLL_TIMEOUT_MS=${7:-5000}
	echo "Creating sink connector with name ${SINK_NAME}, for topic ${TOPIC_NAME}, max tasks ${MAX_TASKS}, tolerance ${TOLERANCE}, for repo ${GRAPHDB_REPO}"
	if ! curl -s --fail localhost:8083/connectors/kafka-sink-graphdb &> /dev/null; then
		curl -H 'Content-Type: application/json' --data '
		{
			"name": "'"${SINK_NAME}"'",
			"config": {
				"connector.class":"com.ontotext.kafka.GraphDBSinkConnector",
				"key.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter.schemas.enable": "false",
				"topics":"'"${TOPIC_NAME}"'",
				"tasks.max":"'"${MAX_TASKS}"'",
				"errors.tolerance" : "'"${TOLERANCE}"'",
				"offset.storage.file.filename": "/tmp/storage",
				"graphdb.server.url": "http://graphdb:7200",
				"graphdb.server.repository": "'"${GRAPHDB_REPO}"'",
				"graphdb.batch.size": "'"${BATCH_SIZE}"'",
				"graphdb.batch.commit.limit.ms": "'"${POLL_TIMEOUT_MS}"'",
				"graphdb.auth.type": "NONE",
				"graphdb.update.type": "ADD",
				"graphdb.update.rdf.format": "nq"
			}
		}' http://localhost:8083/connectors -w "\n"
	fi
}

if command docker-compose &>/dev/null; then
	DOCKER_COMPOSE_CMD="docker-compose"
elif command docker compose &>/dev/null; then
	DOCKER_COMPOSE_CMD="docker compose"
else
	echo "No docker compose command found. Cannot run tests" && exit 1
fi
export DOCKER_COMPOSE_CMD

__dir="$(cd "$(dirname "${BASH_SOURCE[${__b3bp_tmp_source_idx:-0}]}")" && pwd)"
__root="$(cd "${__dir}"/../../ && pwd)"

function start_composition {
	( cd "${__root}"/docker-compose && ${DOCKER_COMPOSE_CMD} up -d connect-1 broker zookeeper graphdb 2>/dev/null)
}

function stop_composition {
	( cd "${__root}"/docker-compose && ${DOCKER_COMPOSE_CMD} down &>/dev/null)
}

function is_number {
	re='^[0-9]+$'
    if ! [[ $1 =~ $re ]] ; then
       return 1
    fi
}

function is_valid_tolerance {
	re='^(all|none)$'
    if ! [[ $1 =~ $re ]] ; then
       return 1
    fi
}

function rand {
	LENGTH=${1:-13}
	tr -dc A-Za-z0-9 </dev/urandom | head -c "${LENGTH}"; echo
}

function min {
	echo $(( $1 < $2 ? $1 : $2 ))
}

function max {
	echo $(( $1 < $2 ? $2 : $1 ))
}

function abs {
	echo ${1#-}
}

function countdown {
	VAL="$1"
    while [[ VAL -gt 0 ]]; do
#       echo -ne "$(date -d@${VAL} -u +%H:%M:%S)\033[0K\r"
       echo -ne "$VAL\033[0K\r"
       sleep 1
       VAL=$(( VAL-1 ))
    done
}
