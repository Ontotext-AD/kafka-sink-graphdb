package com.ontotext.kafka.service;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.util.PropertiesUtil;

public class GraphDBService {
	private static final GraphDBService INSTANCE = new GraphDBService();
	private final AtomicBoolean shouldRun = new AtomicBoolean(true);
	private final AtomicReference<Repository> REPOSITORY = new AtomicReference<>(null);
	private final ConcurrentLinkedQueue<Collection<SinkRecord>> sinkRecords = new ConcurrentLinkedQueue<>();
	private Thread batchProcessor;

	private GraphDBService() {}

	public static GraphDBService connectorService() {
		return INSTANCE;
	}

	public void initialize() {
		if (REPOSITORY.compareAndSet(null, fetchRepository())) {
			batchProcessor = new Thread(new SinkRecordsProcessor(sinkRecords, shouldRun, REPOSITORY.get(),
					Integer.parseInt(PropertiesUtil.getProperty(GraphDBSinkConfig.BATCH_SIZE))));
			batchProcessor.start();
		}
	}

	public void shutDown() {
		shouldRun.set(false);
		// batchProcessor.interrupt(); todo handle if thread still hanging
	}

	public void addData(Collection<SinkRecord> records) {
		sinkRecords.add(records);
	}

	private static Repository fetchRepository() {
		return new HTTPRepository(PropertiesUtil.getProperty(GraphDBSinkConfig.SERVER_IRI),
				PropertiesUtil.getProperty(GraphDBSinkConfig.REPOSITORY));
	}
}
