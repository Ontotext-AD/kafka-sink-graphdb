if [ $# -lt 4 ]
  then
    echo "./startProducer.sh <producer config properties file path> <kafka topic> <files to add data from> <file with keys>..."
    exit 1
fi

mvn clean package -DskipTests

java -cp target/graphdb-kafka-plugin.jar com.ontotext.kafka.producer.RunProducer $@
