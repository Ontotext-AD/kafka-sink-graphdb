cd ~
java -Dfile.encoding=UTF-8 -classpath IdeaProjects/kafka-sink-graphdb/target/classes:.m2/repository/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar:.m2/repository/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar:.m2/repository/org/slf4j/slf4j-simple/1.7.22/slf4j-simple-1.7.22.jar com.ontotext.kafka.producer.GraphDBProducer $@
cd -