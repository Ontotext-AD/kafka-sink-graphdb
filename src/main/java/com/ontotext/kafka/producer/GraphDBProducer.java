package com.ontotext.kafka.producer;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ontotext.kafka.util.ValueUtil;

public class GraphDBProducer<K,V> extends KafkaProducer<K,V>{
    private final List<String> allFiles;
    private final String kafkaTopic;
	private Long updatesPauses = null;
    private static final Logger LOG = LoggerFactory.getLogger(GraphDBProducer.class);
    private String outputMessageFormat = "nq";


    public GraphDBProducer(List<String> files, String kafkaTopic, Properties properties) {
        super(properties);
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
            super.send((ProducerRecord<K, V>) producerRecord, (recordMetadata, e) -> {
                if(e != null) {
                    LOG.error(e.getMessage());
                }
            }).get();
            if (updatesPauses != null) {
                Thread.sleep(updatesPauses);
            }
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
        	LOG.error(e.getMessage(), e);
        }
    }

	private void sendMessage(RDFFormat outputFormat, ByteArrayOutputStream message, String keyName, Model model) {
		if (!couldWriteDirectlyInMessage(outputFormat)) {
			Rio.write(model, message, outputFormat);
			model.clear();
		}
		sendMessage(keyName, message.toByteArray());
	}

	private boolean couldWriteDirectlyInMessage(RDFFormat outputFormat) {
		return outputFormat != RDFFormat.JSONLD && outputFormat != RDFFormat.RDFXML && outputFormat != RDFFormat.TRIX
			&& outputFormat != RDFFormat.BINARY && outputFormat != RDFFormat.NDJSONLD && outputFormat != RDFFormat.RDFJSON;
	}

    public void publish() {
        var outputFormat = ValueUtil.getRDFFormat(this.outputMessageFormat);
        for (int i = 0; i < allFiles.size(); i += 2) {
            String dataFile = allFiles.get(i);
            String keysFile = allFiles.get(i + 1);
            try {
				InputStream dataStream = new FileInputStream(dataFile);

				String ext = FilenameUtils.getExtension(dataFile);
				var inputFormat = ValueUtil.getRDFFormat(ext);

                ByteArrayOutputStream message = new ByteArrayOutputStream();

                try (GraphQueryResult res = QueryResults.parseGraphBackground(dataStream, dataFile, inputFormat, null);
                    BufferedReader keysReader = new BufferedReader(new FileReader(keysFile))) {
                    String keyLine = keysReader.readLine();
                    String keyName = keyLine.split("=")[1];
                    String keyStatements = keysReader.readLine().split("=")[1];
                    int numberOfStatementsPerKey = Integer.parseInt(keyStatements);
                    int countStatementsInCurrentMessage = 0;
					Model model = new TreeModel();

                    while (res.hasNext()) {
                        Statement st = res.next();
						if (!couldWriteDirectlyInMessage(outputFormat)) {
							model.add(st);
						} else {
							Rio.write(st, message, outputFormat);
						}
                        countStatementsInCurrentMessage++;

                        if (countStatementsInCurrentMessage == numberOfStatementsPerKey) {
							sendMessage(outputFormat, message, keyName, model);
							message = new ByteArrayOutputStream();
                            if ( (keyLine = keysReader.readLine()) != null && !(keyLine.startsWith("#"))) {
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
						sendMessage(outputFormat, message, keyName, model);
                    }
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
