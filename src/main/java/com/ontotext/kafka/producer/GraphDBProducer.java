package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class GraphDBProducer<K,V> extends KafkaProducer<K,V>{
    private final List<String> allFiles;
    private final String kafkaTopic;
    private final Properties properties;

    public GraphDBProducer(List<String> files, String kafkaTopic, Properties properties) {
        super(properties);
        this.properties = properties;
        this.allFiles = files;
        this.kafkaTopic = kafkaTopic;
    }

    public void publish() {
        for (String file : this.allFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                while(line != null) {
                    //TODO how to pass key for replace and updates?
                    final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.kafkaTopic, null, line);
                    super.send((ProducerRecord<K, V>) producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null) {
                                e.printStackTrace();
                            }
                        }
                    }).get();
                    line = reader.readLine();
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
