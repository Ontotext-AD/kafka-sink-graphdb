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
        GraphDBProducerConfig config = new GraphDBProducerConfig();
        try {
            props = config.loadConfig(configFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GraphDBProducer<String, String> producer = new GraphDBProducer<>(allFiles, kafkaTopic, props);
        producer.publish();
    }

}
