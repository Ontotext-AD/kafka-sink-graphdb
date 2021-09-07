package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class GraphDBProducer {
    private static String configFile = null;
    private static String kafkaTopic = null;
    private static List<String> allFiles = null;

    private Producer<String, String> getProducer(Properties props) {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Please provide command line arguments: configPath topic filesToReadFrom...");
            System.exit(1);
        }

        configFile = args[0];
        kafkaTopic = args[1];
        allFiles = Arrays.asList(args).subList(2, args.length);
        GraphDBProducer producer = new GraphDBProducer();
        producer.publish();
    }

    private void publish() {
        Properties props = null;
        try {
            props = loadConfig(configFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final Producer<String, String> producer = getProducer(props);

        for (String file : allFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line = reader.readLine();
                while(line != null) {
                    final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(kafkaTopic, null, line);
                    producer.send(producerRecord).get();
                    line = reader.readLine();
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return cfg;
    }
}
