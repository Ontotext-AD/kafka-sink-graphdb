package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class GraphDBProducer<K,V> extends KafkaProducer<K,V>{
    private final List<String> allFiles;
    private final String kafkaTopic;
    private final Properties properties;
    private Long updatesPauses = null;
    private static final Logger LOG = LoggerFactory.getLogger(GraphDBProducer.class);
    private String outputMessageFormat = "nq";


    public GraphDBProducer(List<String> files, String kafkaTopic, Properties properties) {
        super(properties);
        this.properties = properties;
        this.allFiles = files;
        this.kafkaTopic = kafkaTopic;
        if (properties.getProperty("graphdb.updates.time.pauses") != null) {
            this.updatesPauses = Long.parseLong(properties.getProperty("graphdb.updates.time.pauses"));
        }
        if (properties.getProperty("graphdb.message.output.format") != null) {
            this.outputMessageFormat = properties.getProperty("graphdb.message.output.format");
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
        RDFFormat outputFormat = getOutputFormat(this.outputMessageFormat);
        for (int i = 0; i < allFiles.size(); i += 2) {
            String dataFile = allFiles.get(i);
            String keysFile = allFiles.get(i + 1);
            try {
                InputStream dataStream = new FileInputStream(dataFile);
                RDFFormat inputFormat = Rio.getParserFormatForFileName(dataFile).orElse(RDFFormat.TURTLE);

                ByteArrayOutputStream message = new ByteArrayOutputStream();

                try (GraphQueryResult res = QueryResults.parseGraphBackground(dataStream,dataFile, inputFormat);
                     BufferedReader keysReader = new BufferedReader(new FileReader(keysFile))) {
                    String keyLine = keysReader.readLine();
                    String keyName = keyLine.split("=")[1];
                    String keyStatements = keysReader.readLine().split("=")[1];
                    int numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                    int countStatementsInCurrentMessage = 0;

                    while (res.hasNext()) {
                        Statement st = res.next();
                        Rio.write(st, message, outputFormat);
                        countStatementsInCurrentMessage++;

                        if (countStatementsInCurrentMessage == numberOfStatementsPerKey) {
                            sendMessage(keyName, message.toByteArray());
                            message = new ByteArrayOutputStream();
                            if ( (keyLine = keysReader.readLine()) != null) {
                                keyName = keyLine.split("=")[1];
                                keyStatements = keysReader.readLine().split("=")[1];
                                numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                                countStatementsInCurrentMessage = 0;
                            } else {
                                break;
                            }
                        }
                    }
                    if (keyName != null) {
                        sendMessage(keyName, message.toByteArray());
                    }
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private RDFFormat getOutputFormat(String prop) {
        if (RDFFormat.RDFXML.getFileExtensions().contains(prop)) {
            return RDFFormat.RDFXML;
        } else if (RDFFormat.NTRIPLES.getFileExtensions().contains(prop)) {
            return RDFFormat.NTRIPLES;
        } else if (RDFFormat.TURTLE.getFileExtensions().contains(prop)) {
            return RDFFormat.TURTLE;
        } else if (RDFFormat.TURTLESTAR.getFileExtensions().contains(prop)) {
            return RDFFormat.TURTLESTAR;
        } else if (RDFFormat.N3.getFileExtensions().contains(prop)) {
            return RDFFormat.N3;
        } else if (RDFFormat.TRIX.getFileExtensions().contains(prop)) {
            return RDFFormat.TRIX;
        } else if (RDFFormat.TRIG.getFileExtensions().contains(prop)) {
            return RDFFormat.TRIG;
        } else if (RDFFormat.TRIGSTAR.getFileExtensions().contains(prop)) {
            return RDFFormat.TRIGSTAR;
        } else if (RDFFormat.BINARY.getFileExtensions().contains(prop)) {
            return RDFFormat.BINARY;
        } else if (RDFFormat.JSONLD.getFileExtensions().contains(prop)) {
            return RDFFormat.JSONLD;
        } else if (RDFFormat.NDJSONLD.getFileExtensions().contains(prop)) {
            return RDFFormat.NDJSONLD;
        } else if (RDFFormat.RDFJSON.getFileExtensions().contains(prop)) {
            return RDFFormat.RDFJSON;
        } else if (RDFFormat.RDFA.getFileExtensions().contains(prop)) {
            return RDFFormat.RDFA;
        } else if (RDFFormat.HDT.getFileExtensions().contains(prop)) {
            return RDFFormat.HDT;
        } else {
            return RDFFormat.NQUADS;
        }

    }
}
