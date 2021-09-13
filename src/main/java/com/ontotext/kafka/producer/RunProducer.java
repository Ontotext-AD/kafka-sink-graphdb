package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class RunProducer {
    private static final Logger LOG = LoggerFactory.getLogger(RunProducer.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            LOG.error("Please provide command line arguments: configPath topic filesToReadFrom...");
            throw new RuntimeException("Wrong number of command line arguments");
        }

        String configFile = args[0];
        String kafkaTopic = args[1];
        List<String> allFiles = Arrays.asList(args).subList(2, args.length);
        Properties props = null;
        GraphDBProducerConfig config = new GraphDBProducerConfig();
        try {
            props = config.loadConfig(configFile);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        GraphDBProducer<String, String> producer = new GraphDBProducer<>(allFiles, kafkaTopic, props);
        producer.publish();
    }

}
