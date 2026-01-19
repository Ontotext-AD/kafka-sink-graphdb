package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.gdb.GDBConnectionManager;
import com.ontotext.kafka.gdb.GdbConnectionConfig;
import com.ontotext.kafka.logging.LoggerFactory;
import com.ontotext.kafka.logging.LoggingContext;
import com.ontotext.kafka.processor.record.handler.RecordHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
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

	private final Logger log = LoggerFactory.getLogger(SinkRecordsProcessor.class);

	private final String name;
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
	private volatile boolean running = false;

	public static SinkRecordsProcessor create(GraphDBSinkConfig config, String name) {
		return new SinkRecordsProcessor(config, name, new LinkedBlockingQueue<>(), new GDBConnectionManager(new GdbConnectionConfig(config)));
	}


	SinkRecordsProcessor(GraphDBSinkConfig config, String name, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, GDBConnectionManager connectionManager) {
		this(config, name, sinkRecords, connectionManager, new RetryOperator(config));
	}

	SinkRecordsProcessor(GraphDBSinkConfig config, String name, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, GDBConnectionManager connectionManager,
								RetryOperator retryOperator) {
		this(config, name, sinkRecords, new LinkedList<>(), connectionManager, new LogErrorHandler(config), RecordHandler.getRecordHandler(config), retryOperator);
	}

	private SinkRecordsProcessor(GraphDBSinkConfig config, String name, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, Queue<SinkRecord> recordsBatch,
								 GDBConnectionManager connectionManager, LogErrorHandler logErrorHandler, RecordHandler recordHandler,
								 RetryOperator retryOperator) {
		this.config = config;
		this.name = name;
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
		try (LoggingContext context = LoggingContext.withContext("connectorName=" + config.getConnectorName(), "repositoryURL=" + connectionManager.getRepositoryURL())) {
			Throwable errorThrown = null;
			running = true;
			while (shouldRun()) {
				try {
					if (backOff.getAndSet(false)) {
						log.info("Retrying flush");
						flushUpdates(this.recordsBatch);
						log.info("Flush (on retry) successful");
					}
					Collection<SinkRecord> messages = pollForMessages();
					if (messages != null) {
						consumeRecords(messages);
					} else {
						log.trace("Did not receive any records (waited {} {} ) for repository {}. Flushing all records in batch.", timeoutCommitMs,
							TimeUnit.MILLISECONDS, connectionManager);
						flushUpdates(this.recordsBatch);
					}
				} catch (RetriableException e) {
					long backoffTimeoutMs = config.getBackOffTimeoutMs();
					backOff.set(true);
					log.warn("Caught a retriable exception while flushing the current batch. " +
						"Will sleep for {}ms and will try to flush the records again", backoffTimeoutMs);
					try {
						Thread.sleep(backoffTimeoutMs);
					} catch (InterruptedException iex) {
						log.info("Thread was interrupted during backoff. Shutting down");
						break;
					}
				} catch (Throwable e) {
					if (e instanceof InterruptedException) {
						log.info("Thread was interrupted. Shutting down processor");
					} else {
						errorThrown = e;
						log.error("Caught an exception, cannot recover.  Shutting down", e);
					}
					break;
				}
			}
			log.info("Shutting down processor");
			Thread.interrupted();
			shutdown(errorThrown);
		}
	}

	boolean shouldRun() {
		return !Thread.currentThread().isInterrupted() && isRunning();
	}

	Collection<SinkRecord> pollForMessages() throws InterruptedException {
		return sinkRecords.poll(timeoutCommitMs, TimeUnit.MILLISECONDS);
	}

	void shutdown(Throwable errorThrown) {
		SinkProcessorManager.startEviction(name, errorThrown);
		// commit any records left before shutdown. Bypass batch checking, just add all records and flush downstream
		log.info("Commiting any records left before shutdown");
		Collection<SinkRecord> records;
		while ((records = sinkRecords.poll()) != null) {
			recordsBatch.addAll(records);
		}
		try {
			flushUpdates(this.recordsBatch);
		} catch (Exception e) {
			// We did ou best. Just log the exception and shut down
			log.warn("While shutting down, failed to flush updates due to exception", e);
		}

		connectionManager.shutDownRepository();
		running = false;
		SinkProcessorManager.finishEviction(name, errorThrown);

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
			log.trace("Transaction started, batch size: {} , records in current batch: {}", batchSize, recordsInCurrentBatch);
			while (recordsBatch.peek() != null) {
				SinkRecord record = recordsBatch.peek();
				try {
					retryOperator.execute(() -> handleRecord(record, connection));
					consumedRecords.add(recordsBatch.poll());
				} catch (RetriableException e) {
					log.warn("Failed to commit record. Will handle failure, and remove from the batch");
					errorHandler.handleFailingRecord(record, e);
					recordsBatch.poll();
				}
			}
			try {
				connection.commit();
			} catch (RepositoryException e) {
				log.error(
					"Failed to commit transaction due to exception. Restoring consumed records so that they can be flushed later, and rolling back the transaction",
					e);
				recordsBatch.addAll(consumedRecords);
				if (connection.isActive()) {
					connection.rollback();
				}
				throw e;
			}

			log.trace("Transaction commited, Batch size: {} , Records in current batch: {}", batchSize, recordsInCurrentBatch);
			if (log.isTraceEnabled()) {
				long finish = System.currentTimeMillis();
				log.trace("Finished batch processing for {} ms", finish - start);
			}
		} catch (RepositoryException e) {
			throw new RetriableException(e);
		}
	}

	void handleRecord(SinkRecord record, RepositoryConnection connection) {
		long start = System.currentTimeMillis();
		try {
			log.trace("Executing {} operation......", transactionType.toString().toLowerCase());
			recordHandler.handle(record, connection, config);
		} catch (IOException e) {
			throw new RetriableException(e.getMessage(), e);
		} finally {
			if (log.isTraceEnabled()) {
				log.trace("Record info: {}", ValueUtil.recordInfo(record));
				long finish = System.currentTimeMillis();
				log.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
			}
		}

	}

	public String getName() {
		return name;
	}

	public LinkedBlockingQueue<Collection<SinkRecord>> getQueue() {
		return sinkRecords;
	}

	public boolean shouldBackOff() {
		return backOff.get();
	}

	public boolean isRunning() {
		return running;
	}
}
