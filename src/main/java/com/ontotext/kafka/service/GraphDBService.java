package com.ontotext.kafka.service;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.operation.GraphDBOperator;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Global Singleton Service that resolves GraphDB's {@link HTTPRepository} and initializes the {@link SinkRecordsProcessor}
 * based on the {@link GraphDBSinkConfig#TRANSACTION_TYPE}.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBService {
	private static final Logger LOG = LoggerFactory.getLogger(GraphDBService.class);
	private static final GraphDBService INSTANCE = new GraphDBService();
	private final AtomicBoolean shouldRun = new AtomicBoolean(true);
	private final AtomicReference<Repository> repository = new AtomicReference<>(null);
	private final ConcurrentLinkedQueue<Collection<SinkRecord>> sinkRecords = new ConcurrentLinkedQueue<>();
	private ErrorHandler errorHandler;
	private GraphDBOperator operator;
	private Thread recordProcessor;
	private int batchSize;
	private long timeoutCommitMs;

	private GraphDBService() {
	}

	public void initialize(Map<String, ?> properties) {
		if (repository.getAndUpdate(present -> initializeRepository(present, properties)) == null) {
			errorHandler = new LogErrorHandler(properties);
			operator = new GraphDBOperator(properties);
			batchSize = (int) properties.get(GraphDBSinkConfig.BATCH_SIZE);
			timeoutCommitMs = (Long) properties.get(GraphDBSinkConfig.BATCH_COMMIT_SCHEDULER);
			recordProcessor = new Thread(
				fetchProcessor((String) properties.get(GraphDBSinkConfig.TRANSACTION_TYPE),
					(String) properties.get(GraphDBSinkConfig.RDF_FORMAT), (String) properties.get(GraphDBSinkConfig.TEMPLATE_ID)));
			shouldRun.set(true);
			recordProcessor.start();
			LOG.debug("Started the GraphDB Repository connection.");
		}
	}

	public static GraphDBService connectorService() {
		return INSTANCE;
	}

	public void shutDown() {
		try {
			repository.get().shutDown();
		} finally {
			repository.set(null);
			shouldRun.set(false);
			LOG.debug("The GraphDB Repository connection has been terminated.");
		}
	}

	public void addData(Collection<SinkRecord> records) {
		sinkRecords.add(records);
	}

	private Repository initializeRepository(Repository repository, Map<String, ?> properties) {
		return repository == null ? fetchRepository(properties) : repository;
	}

	private static Repository fetchRepository(Map<String, ?> properties) {
		String address = (String) properties.get(GraphDBSinkConfig.SERVER_IRI);
		String repositoryId = (String) properties.get(GraphDBSinkConfig.REPOSITORY);
		var repository = new HTTPRepository(address, repositoryId);
		switch (GraphDBSinkConfig.AuthenticationType.of((String) properties.get(GraphDBSinkConfig.AUTH_TYPE))) {
			case NONE:
				return repository;
			case BASIC:
				repository.setUsernameAndPassword(
					(String) properties.get(GraphDBSinkConfig.AUTH_BASIC_USER),
					((Password) properties.get(GraphDBSinkConfig.AUTH_BASIC_PASS)).value());
				return repository;
			case CUSTOM:
			default:
				throw new UnsupportedOperationException(properties.get(GraphDBSinkConfig.AUTH_TYPE) + " not supported");
		}
	}

	private SinkRecordsProcessor fetchProcessor(String transactionType, String rdfFormat, String templateId) {
		GraphDBSinkConfig.TransactionType type = GraphDBSinkConfig.TransactionType.of(transactionType);
		if (type == null) {
			throw new IllegalArgumentException("Invalid update type: " + transactionType);
		}
		switch (type) {
			case ADD:
				return new AddRecordsProcessor(sinkRecords, shouldRun, repository.get(),
					ValueUtil.getRDFFormat(rdfFormat), batchSize, timeoutCommitMs, errorHandler, operator);
			case REPLACE_GRAPH:
				return new ReplaceGraphProcessor(sinkRecords, shouldRun, repository.get(),
					ValueUtil.getRDFFormat(rdfFormat), batchSize, timeoutCommitMs, errorHandler, operator);
			case SMART_UPDATE:
				return new UpdateRecordsProcessor(sinkRecords, shouldRun, repository.get(),
					ValueUtil.getRDFFormat(rdfFormat), batchSize, timeoutCommitMs, errorHandler, operator, templateId);
			default:
				throw new UnsupportedOperationException("Not implemented yet");
		}
	}
}
