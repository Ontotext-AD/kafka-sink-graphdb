package com.ontotext.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.*;
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
    private Long updatesPauses = null;
    private static final Logger LOG = LoggerFactory.getLogger(GraphDBProducer.class);


    public GraphDBProducer(List<String> files, String kafkaTopic, Properties properties) {
        super(properties);
        this.properties = properties;
        this.allFiles = files;
        this.kafkaTopic = kafkaTopic;
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
            try {
                InputStream dataStream = new FileInputStream(dataFile);
                RDFFormat inputFormat = Rio.getParserFormatForFileName(dataFile).orElse(RDFFormat.TURTLE);

                try (GraphQueryResult res = QueryResults.parseGraphBackground(dataStream,dataFile, inputFormat);
                     BufferedReader keysReader = new BufferedReader(new FileReader(keysFile))) {
                    String keyLine = keysReader.readLine();
                    String keyName = keyLine.split("=")[1];
                    String keyStatements = keysReader.readLine().split("=")[1];
                    int numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                    int countStatementsInCurrentMessage = 0;
                    StringBuilder builder = new StringBuilder();
                    while (res.hasNext()) {
                        Statement st = res.next();
                        convertStatementToNQString(builder, st);
                        countStatementsInCurrentMessage++;
                        if (countStatementsInCurrentMessage == numberOfStatementsPerKey) {
                            sendMessage(keyName, builder.toString().getBytes());
                            if ( (keyLine = keysReader.readLine()) != null) {
                                keyName = keyLine.split("=")[1];
                                keyStatements = keysReader.readLine().split("=")[1];
                                numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                                builder = new StringBuilder();
                                countStatementsInCurrentMessage = 0;
                            } else {
                                break;
                            }
                        }
                    }
                    if (keyName != null) {
                        sendMessage(keyName, builder.toString().getBytes());
                    }
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private void convertStatementToNQString(StringBuilder builder, Statement st) {
        String subj = "<" + st.getSubject().toString() + ">";
        String pred = "<" + st.getPredicate().toString() + ">";
        String obj = "<" + st.getObject().toString() + ">";
        String context = "";

        if (st.getSubject().isBNode()) {
            subj = "_:" + ((BNode) st.getSubject()).getID();
        }

        if (st.getPredicate().isBNode()) {
            pred = "_:" + ((BNode) st.getPredicate()).getID();
        }

        if (st.getObject().isBNode()) {
            obj = "_:" + ((BNode) st.getObject()).getID();
        } else if (st.getObject().isLiteral()) {
            obj = st.getObject().toString();
        }

        if (st.getContext() != null) {
            if (st.getContext().isBNode()) {
                context = "_:" + ((BNode) st.getContext()).getID();
            } else {
                context = "<" + st.getContext().toString() + ">";
            }
        }

        builder.append(subj).append(" ")
                .append(pred).append(" ")
                .append(obj).append(" ").append(context)
                .append(".").append("\n");
    }
}
