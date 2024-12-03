package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

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
	private final SinkTaskContext context;
	private final Queue<SinkRecord> recordsBatch;
	private final Repository repository;
	private final RDFFormat format;
	private final int batchSize;
	private final long timeoutCommitMs;
	private final LogErrorHandler errorHandler;
	private final StringBuilder updateQueryBuilder;
	private final String templateId;
	private final RecordHandler recordHandler;
	private final GraphDBSinkConfig.TransactionType transactionType;
	private final GraphDBSinkConfig config;

	public SinkRecordsProcessor(GraphDBSinkConfig config, SinkTaskContext context) {
		this(config, new LinkedBlockingQueue<>(), initRepository(config), context);
	}

	public SinkRecordsProcessor(GraphDBSinkConfig config, LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords, Repository repository,
								SinkTaskContext context) {
		this.config = config;
		this.sinkRecords = sinkRecords;
		this.context = context;
		this.recordsBatch = new ConcurrentLinkedQueue<>();
		this.repository = repository;
		this.updateQueryBuilder = new StringBuilder();
		this.templateId = config.getTemplateId();
		this.format = config.getRdfFormat();
		this.batchSize = config.getBatchSize();
		this.timeoutCommitMs = config.getProcessorRecordPollTimeoutMs();
		this.errorHandler = new LogErrorHandler(config);
		this.transactionType = config.getTransactionType();
		this.recordHandler = getRecordHandler(transactionType);
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
				Collection<SinkRecord> messages = sinkRecords.poll(timeoutCommitMs, TimeUnit.MILLISECONDS);
				if (messages != null) {
					consumeRecords(messages);
				} else {
					LOG.trace("Did not receive any records (waited {} {}} ) for repository {}. Flushing all records in batch.", timeoutCommitMs,
						TimeUnit.MILLISECONDS, repository);
					flushUpdates();
				}
			} catch (InterruptedException e) {
				LOG.info("Thread was interrupted. Shutting down processor");
				break;
			} catch (Exception e) {
				LOG.error("Caught unhandled exception. Shutting down", e);
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
		flushUpdates();
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
					recordsBatch.clear();
				}
			} catch (ConnectException e) {
				// TODO We would need to push back all records to kafka
				throw new RuntimeException("Got exception while flushing updates, cannot recover", e);
			} catch (Exception e) {
				throw new RuntimeException("Unhandled exception thrown during batch record flush", e);
			}

		}
	}

	public Void doFlush() throws RetriableException {
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
			recordHandler.handle(record, connection);
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

	/**
	 * Handles incoming {@link SinkRecord} based on specified @link {@link com.ontotext.kafka.GraphDBSinkConfig.TransactionType}.
	 * Note: This is not NPE-safe, see @{@link ValueUtil#convertValueToString(Object)}
	 *
	 * @param transactionType Transaction type to use for record handling operation
	 * @return The handler
	 */
	private RecordHandler getRecordHandler(GraphDBSinkConfig.TransactionType transactionType) {
		switch (transactionType) {
			case ADD:
				return (record, connection) -> {
					connection.add(ValueUtil.convertRDFData(record.value()), format);
				};

			case SMART_UPDATE:
				return (record, connection) -> {
					String query = getUpdateQuery(record.key());
					connection.prepareUpdate(query).execute();
					connection.add(ValueUtil.convertRDFData(record.value()), format);
				};

			case REPLACE_GRAPH:
				return (record, connection) -> {
					Resource context = ValueUtil.convertIRIKey(record.key());
					connection.clear(context);
					LOG.trace("Connection cleared context(IRI): {}", context.stringValue());
					if (record.value() != null) {
						connection.add(ValueUtil.convertRDFData(record.value()), format, context);
					}
				};
			default:
				throw new NotImplementedException(String.format("Handler for transaction type %s not implemented", transactionType));

		}
	}

	private String getUpdateQuery(Object key) {
		String templateBinding = convertValueToString(key);

		updateQueryBuilder.setLength(0);
		return updateQueryBuilder.append("PREFIX onto: <http://www.ontotext.com/>\n")
			.append("insert data {\n")
			.append("    onto:smart-update onto:sparql-template <").append(templateId).append(">;\n")
			.append("               onto:template-binding-id <").append(templateBinding).append("> .\n")
			.append("}\n")
			.toString();
	}

	public UUID getId() {
		return id;
	}

	public LinkedBlockingQueue<Collection<SinkRecord>> getQueue() {
		return sinkRecords;
	}

	private interface RecordHandler {
		void handle(SinkRecord record, RepositoryConnection connection) throws IOException;
	}
}
