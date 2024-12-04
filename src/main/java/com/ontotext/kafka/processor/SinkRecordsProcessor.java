package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A processor which batches sink records and flushes the updates to a given
 * GraphDB {@link org.eclipse.rdf4j.repository.http.HTTPRepository}.
 * <p>
 * Batches that do not meet the {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_SIZE} are flushed
 * after passing {@link com.ontotext.kafka.GraphDBSinkConfig#RECORD_POLL_TIMEOUT} threshold.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public final class SinkRecordsProcessor implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(SinkRecordsProcessor.class);

	private final UUID id = UUID.randomUUID();
	private final LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords;
	private final Queue<SinkRecord> recordsBatch;
	private final Repository repository;
	private final int batchSize;
	private final long timeoutCommitMs;
	private final LogErrorHandler errorHandler;
	private final RecordHandler recordHandler;
	private final GraphDBSinkConfig.TransactionType transactionType;
	private final GraphDBSinkConfig config;
	private AtomicBoolean backOff = new AtomicBoolean(false);
	private static final Map<GraphDBSinkConfig.TransactionType, RecordHandler> RECORD_HANDLERS = new HashMap<>();

	static {
		RECORD_HANDLERS.put(GraphDBSinkConfig.TransactionType.ADD, RecordHandler.addHandler());
		RECORD_HANDLERS.put(GraphDBSinkConfig.TransactionType.REPLACE_GRAPH, RecordHandler.replaceHandler());
		RECORD_HANDLERS.put(GraphDBSinkConfig.TransactionType.SMART_UPDATE, RecordHandler.updateHandler());
	}

	public SinkRecordsProcessor(GraphDBSinkConfig config, SinkTaskContext context) {
		this(config, new LinkedBlockingQueue<>(), initRepository(config));
	}

	public SinkRecordsProcessor(GraphDBSinkConfig config, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, Repository repository) {
		this.config = config;
		this.sinkRecords = sinkRecords;
		this.recordsBatch = new ConcurrentLinkedQueue<>();
		this.repository = repository;
		this.batchSize = config.getBatchSize();
		this.timeoutCommitMs = config.getProcessorRecordPollTimeoutMs();
		this.errorHandler = new LogErrorHandler(config);
		this.transactionType = config.getTransactionType();
		this.recordHandler = RECORD_HANDLERS.get(transactionType);
		// repositoryUrl is used for logging purposes
		String repositoryUrl = (repository instanceof HTTPRepository) ? ((HTTPRepository) repository).getRepositoryURL() : "unknown";
		MDC.put("RepositoryURL", repositoryUrl);
		MDC.put("Connector", config.getConnectorName());
	}


	private static Repository initRepository(GraphDBSinkConfig config) {
		String address = config.getServerUrl();
		String repositoryId = config.getRepositoryId();
		GraphDBSinkConfig.AuthenticationType authType = config.getAuthType();
		HTTPRepository repository = new HTTPRepository(address, repositoryId);
		switch (authType) {
			case NONE:
				return repository;
			case BASIC:
				if (LOG.isTraceEnabled()) {
					LOG.trace("Initializing repository connection with user {}", config.getAuthBasicUser());
				}
				repository.setUsernameAndPassword(config.getAuthBasicUser(), config.getAuthBasicPassword().value());
				return repository;
			case CUSTOM:
			default: // Any other types which are valid, as per definition, but are not implemented yet
				throw new UnsupportedOperationException(authType + " not supported");
		}
	}

	@Override
	public void run() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				if (backOff.getAndSet(false)) {
					LOG.info("Retrying flush");
					flushUpdates();
				}
				Collection<SinkRecord> messages = sinkRecords.poll(timeoutCommitMs, TimeUnit.MILLISECONDS);
				if (messages != null) {
					consumeRecords(messages);
				} else {
					LOG.trace("Did not receive any records (waited {} {}} ) for repository {}. Flushing all records in batch.", timeoutCommitMs,
						TimeUnit.MILLISECONDS, repository);
					flushUpdates();
				}
			} catch (RetriableException e) {
				long backoffTimeoutMs = config.getBackOffTimeoutMs();
				backOff.set(true);
				LOG.warn("Caught a retriable exception while flushing the current batch. " +
					"Will sleep for {}ms and will try to flush the records again", backoffTimeoutMs);
				try {
					Thread.sleep(backoffTimeoutMs);
				} catch (InterruptedException iex) {
					LOG.info("Thread was interrupted during backoff. Shutting down");
					break;
				}


			} catch (Exception e) {
				if (e instanceof InterruptedException) {
					LOG.info("Thread was interrupted. Shutting down processor");
				} else {
					LOG.error("Caught an exception, cannot recover.  Shutting down", e);
				}
				break;
			}
		}
		Thread.interrupted();
		shutdown();
	}

	private void shutdown() {
		// commit any records left before shutdown. Bypass batch checking, just add all records and flush downstream
		LOG.info("Commiting any records left before shutdown");
		Collection<SinkRecord> records;
		while ((records = sinkRecords.poll()) != null) {
			recordsBatch.addAll(records);
		}
		try {
			flushUpdates();
		} catch (Exception e) {
			// We did ou best. Just log the exception and shut down
			LOG.warn("While shutting down, failed to flush updates due to exception", e);
		}

		if (repository.isInitialized()) {
			repository.shutDown();
		}
	}

	private void consumeRecords(Collection<SinkRecord> messages) {
		for (SinkRecord message : messages) {
			recordsBatch.add(message);
			if (batchSize <= recordsBatch.size()) {
				flushUpdates();
			}
		}
	}

	/**
	 * Flush all records in batch downstream.
	 *
	 * @throws RetriableException if the repository connection has failed, either during initialization, or commit
	 * @throws ConnectException   if the flush has failed, but there is no tolerance for error
	 */
	private void flushUpdates() {
		if (!recordsBatch.isEmpty()) {
			try (GraphDBRetryWithToleranceOperator retryOperator = new GraphDBRetryWithToleranceOperator(config)) {
				retryOperator.execute(this::doFlush, Stage.KAFKA_CONSUME, getClass());
				if (retryOperator.failed()) {
					LOG.error("Failed to flush batch updates. Underlying exception - {}", retryOperator.error().getMessage());
					if (!retryOperator.withinToleranceLimits()) {
						throw new ConnectException("Error tolerance exceeded.");
					}
					LOG.warn("Errors are tolerated (tolerance = {}). Clearing the batch", config.getTolerance());
					// TODO: this is not optimal - we clear all reacords when they fail to flush downstream. THink of a way to retry again with some backoff
//					recordsBatch.clear();
					throw new RetriableException("Failed to flush updates", retryOperator.error());
				}
			}
		}
	}

	/**
	 * Perform flush with a retry mechanism, for both the entire batch, and every single record
	 *
	 * @throws RetriableException if the repository connection has failed, either during initialization, or commit
	 * @throws ConnectException   if the flush has failed, but there is no tolerance for error
	 */
	public Void doFlush() {
		long start = System.currentTimeMillis();
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			int recordsInCurrentBatch = recordsBatch.size();
			LOG.trace("Transaction started, batch size: {} , records in current batch: {}", batchSize, recordsInCurrentBatch);
			while (recordsBatch.peek() != null) {
				SinkRecord record = recordsBatch.peek();
				try (GraphDBRetryWithToleranceOperator retryOperator = new GraphDBRetryWithToleranceOperator(config)) {
					retryOperator.execute(() -> handleRecord(record, connection), Stage.KAFKA_CONSUME, getClass());
					if (retryOperator.failed()) {
						LOG.warn("Failed to commit record. Will handle failure, and remove from the batch");
						errorHandler.handleFailingRecord(record, retryOperator.error());
						if (!retryOperator.withinToleranceLimits()) {
							throw new ConnectException("Error tolerance exceeded.");
						}
					}
					recordsBatch.poll();
				}

			}
			connection.commit();
			LOG.trace("Transaction commited, Batch size: {} , Records in current batch: {}", batchSize, recordsInCurrentBatch);
			if (LOG.isTraceEnabled()) {
				long finish = System.currentTimeMillis();
				LOG.trace("Finished batch processing for {} ms", finish - start);
			}
			return null;
		} catch (RepositoryException e) {
			throw new RetriableException(e);
		}
	}

	Void handleRecord(SinkRecord record, RepositoryConnection connection) {
		long start = System.currentTimeMillis();
		try {
			LOG.trace("Executing {} operation......", transactionType.toString().toLowerCase());
			recordHandler.handle(record, connection, config);
			return null;
		} catch (IOException e) {
			throw new RetriableException(e.getMessage(), e);
		} finally {
			if (LOG.isTraceEnabled()) {
				LOG.trace("Record info: {}", ValueUtil.recordInfo(record));
				long finish = System.currentTimeMillis();
				LOG.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
			}
		}

	}

	public UUID getId() {
		return id;
	}

	public LinkedBlockingQueue<Collection<SinkRecord>> getQueue() {
		return sinkRecords;
	}

	public boolean shouldBackOff() {
		return backOff.get();
	}
}
