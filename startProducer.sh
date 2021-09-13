if [ $# -lt 4 ]
  then
    echo "./startProducer.sh <project dir> <producer config properties file path> <kafka topic> <file with keys> <files to add data from> ..."
    exit 1
fi

cd "$1/target/"
shift

java -cp graphdb-kafka-plugin.jar com.ontotext.kafka.producer.RunProducer $@
rm graphdb-kafka-plugin.jar