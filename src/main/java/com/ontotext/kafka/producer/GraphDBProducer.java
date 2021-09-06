package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class GraphDBProducer {
    private static String kafkaBrokerEndpoint = null;
    private static String kafkaTopic = null;
    private static String file = null;

    private Producer<String, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "GraphDBProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) {
        if (args != null) {
            kafkaBrokerEndpoint = args[0];
            kafkaTopic = args[1];
            file = args[2];
        }

        GraphDBProducer producer = new GraphDBProducer();
        producer.publish();
    }

    private void publish() {
        final Producer<String, String> producer = getProducer();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            while(line != null) {
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(kafkaTopic, UUID.randomUUID().toString(), line);
                producer.send(producerRecord).get();
                line = reader.readLine();
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
