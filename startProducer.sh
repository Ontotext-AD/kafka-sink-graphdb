if [ $# -lt 4 ]
  then
    echo "./startProducer.sh <plugins dir> <producer config properties file path> <kafka topic> <files to add data from> ..."
    exit 1
fi

cd "$1/kafka-sink-graphdb/"
shift
java -cp kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar:kafka-sink-graphdb.jar com.ontotext.kafka.producer.RunProducer $@
