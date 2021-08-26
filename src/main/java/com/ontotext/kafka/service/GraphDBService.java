package com.ontotext.kafka.service;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.convert.RecordConverter;
import com.ontotext.kafka.util.PropertiesUtil;

public class GraphDBService {
	private static final GraphDBService INSTANCE = new GraphDBService();
	private final AtomicBoolean shouldRun = new AtomicBoolean(true);
	private final AtomicReference<Repository> repository = new AtomicReference<>(null);
	private final ConcurrentLinkedQueue<Collection<SinkRecord>> sinkRecords = new ConcurrentLinkedQueue<>();
	private Thread batchProcessor;

	private GraphDBService() {}

	public void initialize(RDFFormat format, String address, String repositoryId, RecordConverter converter) {
		if (repository.compareAndSet(null, fetchRepository(address, repositoryId))) {
			batchProcessor = new Thread(
					new SinkRecordsProcessor(sinkRecords, shouldRun, repository.get(), format, converter,
							Integer.parseInt(PropertiesUtil.getProperty(GraphDBSinkConfig.BATCH_SIZE))));
			batchProcessor.start();
		}
	}

	public static GraphDBService connectorService() {
		return INSTANCE;
	}

	public void shutDown() {
		shouldRun.set(false);
		// batchProcessor.interrupt(); todo handle if thread still hanging
	}

	public void addData(Collection<SinkRecord> records) {
		sinkRecords.add(records);
	}

	private static Repository fetchRepository(String address, String repositoryId) {
		return new HTTPRepository(address, repositoryId);
	}
}
