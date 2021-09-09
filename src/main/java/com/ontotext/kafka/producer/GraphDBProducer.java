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
    private final String keySeparator;

    public GraphDBProducer(List<String> files, String kafkaTopic, Properties properties) {
        super(properties);
        this.properties = properties;
        this.allFiles = files;
        this.kafkaTopic = kafkaTopic;
        this.keySeparator = properties.getProperty("graphdb.key.separator");
    }

    public void publish() {
        for (String file : this.allFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                String key = null;
                if (this.keySeparator != null && line.contains(this.keySeparator)) {
                    key = line.split(this.keySeparator)[0];
                    line = line.split(this.keySeparator)[1];
                }
                StringBuilder builder = new StringBuilder();
                while(line != null) {
                    builder.append(line).append("\n");
                    line = reader.readLine();
                }
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.kafkaTopic, key, builder.toString());
                super.send((ProducerRecord<K, V>) producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            e.printStackTrace();
                        }
                    }
                }).get();

            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
