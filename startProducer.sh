if [ $# -lt 4 ]
  then
    echo "./startProducer.sh <plugins dir> <producer config properties file path> <kafka topic> <files to add data from> ..."
    exit 1
fi

cd "$1/kafka-sink-graphdb/"
shift
#TODO set name for jar
java -cp kafka-sink-graphdb-1.0-SNAPSHOT.jar com.ontotext.kafka.producer.RunProducer $@