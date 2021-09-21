if [ $# -lt 4 ]
  then
    echo "./startProducer.sh <producer config properties file path> <kafka topic> <file with keys> <files to add data from> ..."
    exit 1
fi

mvn clean install -DskipTests

CURR_DIR=$(pwd)

echo ${CURR_DIR}

java -cp ${CURR_DIR}/target/graphdb-kafka-plugin.jar com.ontotext.kafka.producer.RunProducer $@
