if [ $# -lt 4 ]
  then
    echo "./startProducer.sh <producer config properties file path> <kafka topic> <file with keys> <files to add data from> ..."
    exit 1
fi

cd "target/"

java -cp graphdb-kafka-plugin.jar com.ontotext.kafka.producer.RunProducer $@