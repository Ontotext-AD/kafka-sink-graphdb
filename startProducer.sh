#cd ~
#java -classpath IdeaProjects/kafka-sink-graphdb/target/classes:.m2/repository/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar:.m2/repository/org/apache/kafka/kafka-clients/2.4.0/kafka-clients-2.4.0.jar:.m2/repository/org/slf4j/slf4j-simple/1.7.22/slf4j-simple-1.7.22.jar com.ontotext.kafka.producer.GraphDBProducer $@
#cd -
#java -cp kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar:kafka-sink-graphdb.jar com/ontotext/kafka/producer/RunProducer $@
# $@ = /home/vasil/kafka_2.13-2.8.0/config/graphdb-producer.properties graphdb testKafka.ttl
java -cp kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar:kafka-sink-graphdb.jar com/ontotext/kafka/producer/GraphDBProducer $@
