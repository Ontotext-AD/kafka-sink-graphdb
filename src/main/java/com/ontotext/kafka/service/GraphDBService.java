package com.ontotext.kafka.service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.ontotext.kafka.error.LogErrorHandler;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.util.ValueUtil;

/**
 * Global Singleton Service that resolves GraphDB's {@link HTTPRepository} and initializes the {@link SinkRecordsProcessor}
 * based on the {@link GraphDBSinkConfig#TRANSACTION_TYPE}.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBService {
	private static final GraphDBService INSTANCE = new GraphDBService();
	private final AtomicBoolean shouldRun = new AtomicBoolean(true);
	private final AtomicReference<Repository> repository = new AtomicReference<>(null);
	private final ConcurrentLinkedQueue<Collection<SinkRecord>> sinkRecords = new ConcurrentLinkedQueue<>();
	private ErrorHandler errorHandler;
	private Thread recordProcessor;
	private int batchSize;
	private long timeoutCommitMs;

	private GraphDBService() {}

	public void initialize(Map<String, String> properties) {
		if (repository.compareAndSet(null, fetchRepository(properties))) {
			batchSize = Integer.parseInt(properties.get(GraphDBSinkConfig.BATCH_SIZE));
			timeoutCommitMs = Long.parseLong(properties.get(GraphDBSinkConfig.BATCH_COMMIT_SCHEDULER));
			errorHandler = new LogErrorHandler();

			recordProcessor = new Thread(
					fetchProcessor(properties.get(GraphDBSinkConfig.TRANSACTION_TYPE),
							properties.get(GraphDBSinkConfig.RDF_FORMAT)));
			recordProcessor.start();
		}
	}

	public static GraphDBService connectorService() {
		return INSTANCE;
	}

	public void shutDown() {
		shouldRun.set(false);
	}

	public void addData(Collection<SinkRecord> records) {
		sinkRecords.add(records);
	}

	private static Repository fetchRepository(Map<String, String> properties) {
		String address = properties.get(GraphDBSinkConfig.SERVER_IRI);
		String repositoryId = properties.get(GraphDBSinkConfig.REPOSITORY);
		var repository = new HTTPRepository(address, repositoryId);
		switch (GraphDBSinkConfig.AuthenticationType.of(properties.get(GraphDBSinkConfig.AUTH_TYPE))) {
			case NONE:
				return repository;
			case BASIC:
				repository.setUsernameAndPassword(properties.get(GraphDBSinkConfig.AUTH_BASIC_USER),
						properties.get(GraphDBSinkConfig.AUTH_BASIC_PASS));
				return repository;
			case CUSTOM:
			default:
				throw new UnsupportedOperationException(properties.get(GraphDBSinkConfig.AUTH_TYPE) + " not supported");
		}
	}

	private SinkRecordsProcessor fetchProcessor(String transactionType, String rdfFormat) {
		GraphDBSinkConfig.TransactionType type = GraphDBSinkConfig.TransactionType.of(transactionType);
		if (type == null) {
			throw new IllegalArgumentException("Invalid transaction type: " + transactionType);
		}
		switch (type) {
			case ADD:
				return new AddRecordsProcessor(sinkRecords, shouldRun, repository.get(),
						ValueUtil.getRDFFormat(rdfFormat), batchSize, timeoutCommitMs, errorHandler);
			case SMART_UPDATE:
				return new UpdateRecordsProcessor(sinkRecords, shouldRun, repository.get(),
						ValueUtil.getRDFFormat(rdfFormat), batchSize, timeoutCommitMs, errorHandler);

			case REPLACE_GRAPH:
			default:
				throw new UnsupportedOperationException("Not implemented yet");
		}
	}
}