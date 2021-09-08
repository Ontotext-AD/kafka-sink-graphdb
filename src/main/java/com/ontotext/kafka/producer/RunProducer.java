package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class RunProducer {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Please provide command line arguments: configPath topic filesToReadFrom...");
            System.exit(1);
        }

        String configFile = args[0];
        String kafkaTopic = args[1];
        List<String> allFiles = Arrays.asList(args).subList(2, args.length);
        Properties props = null;
        try {
            props = loadConfig(configFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GraphDBProducer<String, String> producer = new GraphDBProducer<>(allFiles, kafkaTopic, props);
        producer.publish();
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
