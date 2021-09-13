package com.ontotext.kafka.producer;

import com.ontotext.load.GraphdbRDFLoader;
import com.ontotext.trree.GraphDBParserConfig;
import org.apache.kafka.clients.producer.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

    private void sendMessage(String key, byte[] data) {
        byte[] messageKey = null;
        if (key != null && !key.equals("null")) {
            messageKey = key.getBytes();
        }
        final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(this.kafkaTopic, messageKey, data);
        try {
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
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(e.getMessage());
        }
    }

    public void publish() {
        for (int i = 0; i < allFiles.size(); i += 2) {
            String dataFile = allFiles.get(i);
            String keysFile = allFiles.get(i + 1);
            File tempFile = null;
            try {
                tempFile = File.createTempFile("prefix-", "-suffix");

                InputStream dataStream = new FileInputStream(dataFile);
                RDFFormat inputFormat = Rio.getParserFormatForFileName(dataFile).orElse(RDFFormat.TURTLE);

                RDFParser rdfParser = Rio.createParser(inputFormat);
                RDFWriter rdfWriter = Rio.createWriter(RDFFormat.NQUADS,
                        new FileOutputStream(tempFile.getName()));

                rdfParser.setRDFHandler(rdfWriter);
                rdfParser.parse(dataStream, dataStream.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }


//            try {
//                InputStream dataStream = new FileInputStream(dataFile);
//                RDFFormat inputFormat = Rio.getParserFormatForFileName(dataFile).orElse(RDFFormat.TURTLE);
//
//                GraphdbRDFLoader loader = new GraphdbRDFLoader(GraphDBParserConfig.newInstance(), SimpleValueFactory.getInstance());
//                try {
//                    tempFile = File.createTempFile("prefix-", "-suffix");
//                    RDFWriter rdfWriter = Rio.createWriter(RDFFormat.NQUADS,
//                            new FileOutputStream(tempFile.getName()));
//                    loader.load(new FileInputStream(dataFile), null, inputFormat, rdfWriter);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//

            try (BufferedReader dataReader = new BufferedReader(new FileReader(tempFile.getName()));
                BufferedReader keysReader = new BufferedReader(new FileReader(keysFile))) {
                String line = dataReader.readLine();
                String keyName = keysReader.readLine().split("=")[1];
                String keyStatements = keysReader.readLine().split("=")[1];
                int numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                StringBuilder builder = new StringBuilder();
                int countStatementsInCurrentMessage = 0;
                while(line != null) {
                    builder.append(line).append("\n");
                    countStatementsInCurrentMessage++;
                    if (countStatementsInCurrentMessage == numberOfStatementsPerKey) {
                        sendMessage(keyName, builder.toString().getBytes());
                        if ( (keyName = keysReader.readLine()) != null) {
                            keyStatements = keysReader.readLine().split("=")[1];
                            numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                            builder = new StringBuilder();
                            countStatementsInCurrentMessage = 0;
                        }
                    }
                    line = dataReader.readLine();
                }
                sendMessage(keyName, builder.toString().getBytes());

            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }
}
