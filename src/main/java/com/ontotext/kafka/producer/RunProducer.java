package com.ontotext.kafka.producer;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RunProducer {
//    private static final Logger LOG = LoggerFactory.getLogger(RunProducer.class);

	// FIXME: what is this class?! Is this the entry point? Either document or remove it.
    public static void main(String[] args) {
        if (args.length < 3) {
//            LOG.error("Please provide command line arguments: configPath topic filesToReadFrom...");
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
//            LOG.error(e.getMessage(), e);
        }
		// FIXME: this is auto closable and has to be used in try with resources
        GraphDBProducer<String, String> producer = new GraphDBProducer<>(allFiles, kafkaTopic, props);
        producer.publish();
    }

}
