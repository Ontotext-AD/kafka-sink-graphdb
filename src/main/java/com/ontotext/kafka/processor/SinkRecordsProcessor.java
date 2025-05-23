package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.gdb.GDBConnectionManager;
import com.ontotext.kafka.gdb.GdbConnectionConfig;
import com.ontotext.kafka.processor.record.handler.RecordHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
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
	private final GDBConnectionManager connectionManager;
	private final int batchSize;
	private final long timeoutCommitMs;
	private final LogErrorHandler errorHandler;
	private final RecordHandler recordHandler;
	private final GraphDBSinkConfig.TransactionType transactionType;
	private final GraphDBSinkConfig config;
	private final AtomicBoolean backOff = new AtomicBoolean(false);
	private final RetryOperator retryOperator;

	public SinkRecordsProcessor(GraphDBSinkConfig config) {
		this(config, new LinkedBlockingQueue<>(), new GDBConnectionManager(new GdbConnectionConfig(config)));
	}

	public SinkRecordsProcessor(GraphDBSinkConfig config, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, GDBConnectionManager connectionManager) {
		this(config, sinkRecords, connectionManager, new RetryOperator(config));
	}


	public SinkRecordsProcessor(GraphDBSinkConfig config, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, GDBConnectionManager connectionManager,
								RetryOperator retryOperator) {
		this(config, sinkRecords, new LinkedList<>(), connectionManager, new LogErrorHandler(config), RecordHandler.getRecordHandler(config), retryOperator);
	}

	private SinkRecordsProcessor(GraphDBSinkConfig config, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, Queue<SinkRecord> recordsBatch,
								 GDBConnectionManager connectionManager, LogErrorHandler logErrorHandler, RecordHandler recordHandler,
								 RetryOperator retryOperator) {
		this.config = config;
		this.sinkRecords = sinkRecords;
		this.connectionManager = connectionManager;
		this.errorHandler = logErrorHandler;
		this.recordHandler = recordHandler;
		this.retryOperator = retryOperator;
		this.recordsBatch = recordsBatch;
		this.batchSize = config.getBatchSize();
		this.timeoutCommitMs = config.getProcessorRecordPollTimeoutMs();
		this.transactionType = config.getTransactionType();

		// repositoryUrl is used for logging purposes
		MDC.put("RepositoryURL", connectionManager.getRepositoryURL());
		MDC.put("Connector", config.getConnectorName());
	}

	@Override
	public void run() {
		while (shouldRun()) {
			try {
				if (backOff.getAndSet(false)) {
					LOG.info("Retrying flush");
					flushUpdates(this.recordsBatch);
					LOG.info("Flush (on retry) successful");
				}
				Collection<SinkRecord> messages = pollForMessages();
				if (messages != null) {
					consumeRecords(messages);
				} else {
					LOG.trace("Did not receive any records (waited {} {} ) for repository {}. Flushing all records in batch.", timeoutCommitMs,
						TimeUnit.MILLISECONDS, connectionManager);
					flushUpdates(this.recordsBatch);
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

	boolean shouldRun() {
		return !Thread.currentThread().isInterrupted();
	}

	Collection<SinkRecord> pollForMessages() throws InterruptedException {
		return sinkRecords.poll(timeoutCommitMs, TimeUnit.MILLISECONDS);
	}

	void shutdown() {
		// commit any records left before shutdown. Bypass batch checking, just add all records and flush downstream
		LOG.info("Commiting any records left before shutdown");
		Collection<SinkRecord> records;
		while ((records = sinkRecords.poll()) != null) {
			recordsBatch.addAll(records);
		}
		try {
			flushUpdates(this.recordsBatch);
		} catch (Exception e) {
			// We did ou best. Just log the exception and shut down
			LOG.warn("While shutting down, failed to flush updates due to exception", e);
		}

		connectionManager.shutDownRepository();
	}

	void consumeRecords(Collection<SinkRecord> messages) {
		for (SinkRecord message : messages) {
			recordsBatch.add(message);
			if (batchSize <= recordsBatch.size()) {
				flushUpdates(recordsBatch);
			}
		}
	}

	/**
	 * Flush all records in batch downstream.
	 *
	 * @param recordsBatch The batch of records to flush downstream
	 * @throws RetriableException if the repository connection has failed, either during initialization, or commit
	 * @throws ConnectException   if the flush has failed, but there is no tolerance for error
	 */
	void flushUpdates(Queue<SinkRecord> recordsBatch) {
		if (!recordsBatch.isEmpty()) {
			retryOperator.execute(() -> doFlush(recordsBatch));
		}
	}

	/**
	 * Perform flush with a retry mechanism, for both the entire batch, and every single record
	 *
	 * @param recordsBatch The batch of records to flush
	 * @throws RetriableException if the repository connection has failed, either during initialization, or commit
	 * @throws ConnectException   if the flush has failed, but there is no tolerance for error
	 */
	void doFlush(Queue<SinkRecord> recordsBatch) {
		// Keep a copy of all consumed records, so that records are not lost if the transaction fails to commit
		Collection<SinkRecord> consumedRecords = new ArrayList<>();
		long start = System.currentTimeMillis();
		try (RepositoryConnection connection = connectionManager.newConnection()) {
			connection.begin();
			int recordsInCurrentBatch = recordsBatch.size();
			LOG.trace("Transaction started, batch size: {} , records in current batch: {}", batchSize, recordsInCurrentBatch);
			while (recordsBatch.peek() != null) {
				SinkRecord record = recordsBatch.peek();
				try {
					retryOperator.execute(() -> handleRecord(record, connection));
					consumedRecords.add(recordsBatch.poll());
				} catch (RetriableException e) {
					LOG.warn("Failed to commit record. Will handle failure, and remove from the batch");
					errorHandler.handleFailingRecord(record, e);
					recordsBatch.poll();
				}
			}
			try {
				connection.commit();
			} catch (RepositoryException e) {
				LOG.error(
					"Failed to commit transaction due to exception. Restoring consumed records so that they can be flushed later, and rolling back the transaction",
					e);
				recordsBatch.addAll(consumedRecords);
				if (connection.isActive()) {
					connection.rollback();
				}
				throw e;
			}

			LOG.trace("Transaction commited, Batch size: {} , Records in current batch: {}", batchSize, recordsInCurrentBatch);
			if (LOG.isTraceEnabled()) {
				long finish = System.currentTimeMillis();
				LOG.trace("Finished batch processing for {} ms", finish - start);
			}
		} catch (RepositoryException e) {
			throw new RetriableException(e);
		}
	}

	void handleRecord(SinkRecord record, RepositoryConnection connection) {
		long start = System.currentTimeMillis();
		try {
			LOG.trace("Executing {} operation......", transactionType.toString().toLowerCase());
			recordHandler.handle(record, connection, config);
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
