package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class GraphDBProducer<K,V> extends KafkaProducer<K,V>{
    private final List<String> allFiles;
    private final String kafkaTopic;
    private final Properties properties;
    private final String keySeparator;
    private Long updatesPauses = null;
    private static final Logger LOG = LoggerFactory.getLogger(GraphDBProducer.class);


    public GraphDBProducer(List<String> files, String kafkaTopic, Properties properties) {
        super(properties);
        this.properties = properties;
        this.allFiles = files;
        this.kafkaTopic = kafkaTopic;
        this.keySeparator = properties.getProperty("graphdb.key.separator");
        if (properties.getProperty("graphdb.updates.time.pauses") != null) {
            this.updatesPauses = Long.parseLong(properties.getProperty("graphdb.updates.time.pauses"));
        }
    }

    public void publish() {
        for (String file : this.allFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                byte[] key = null;
                if (this.keySeparator != null && line.contains(this.keySeparator)) {
                    key = line.split(this.keySeparator)[0].getBytes();
                    line = line.split(this.keySeparator)[1];
                }
                StringBuilder builder = new StringBuilder();
                while(line != null) {
                    builder.append(line).append("\n");
                    line = reader.readLine();
                }
                final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(this.kafkaTopic, key, builder.toString().getBytes());
                super.send((ProducerRecord<K, V>) producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            LOG.error(e.getMessage());
                        }
                    }
                }).get();
                if (updatesPauses != null) {
                    Thread.sleep(updatesPauses);
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                LOG.error(e.getMessage());
            }
        }
    }
}
