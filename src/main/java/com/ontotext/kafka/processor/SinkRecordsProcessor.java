package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.operation.GraphDBOperator;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

/**
 * A processor which batches sink records and flushes the updates to a given
 * GraphDB {@link org.eclipse.rdf4j.repository.http.HTTPRepository}.
 * <p>
 * Batches that do not meet the {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_SIZE} are flushed
 * after passing {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_COMMIT_SCHEDULER} threshold.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public final class SinkRecordsProcessor implements Runnable, Operation<Object> {

	private static final Logger LOG = LoggerFactory.getLogger(SinkRecordsProcessor.class);
	private static final Object SUCCESSES = new Object();

	private final UUID id = UUID.randomUUID();
	private final LinkedBlockingDeque<Collection<SinkRecord>> sinkRecords;
	private final Queue<SinkRecord> recordsBatch;
	private final Repository repository;
	private final RDFFormat format;
	private final ReentrantLock timeoutCommitLock;
	private final int batchSize;
	private final long timeoutCommitMs;
	private final ErrorHandler errorHandler;
	private final Set<SinkRecord> failedRecords;
	private final OperationHandler operator;
	private final StringBuilder updateQueryBuilder;
	private final String templateId;
	private final RecordHandler recordHandler;
	private final GraphDBSinkConfig.TransactionType transactionType;

	public SinkRecordsProcessor(GraphDBSinkConfig config) {
		this(config, new LinkedBlockingDeque<>(), initRepository(config));
	}

	public SinkRecordsProcessor(GraphDBSinkConfig config, LinkedBlockingDeque<Collection<SinkRecord>> sinkRecords, Repository repository) {
		this.sinkRecords = sinkRecords;
		this.recordsBatch = new ConcurrentLinkedQueue<>();
		this.repository = repository;
		this.updateQueryBuilder = new StringBuilder();
		this.templateId = config.getTemplateId();
		this.format = config.getRdfFormat();
		this.batchSize = config.getBatchSize();
		this.timeoutCommitMs = config.getTimeoutCommitMs();
		this.errorHandler = new LogErrorHandler(config);
		this.operator = new GraphDBOperator(config);
		this.timeoutCommitLock = new ReentrantLock();
		this.failedRecords = new HashSet<>();
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
				Collection<SinkRecord> messages = sinkRecords.pollLast(timeoutCommitMs, TimeUnit.MILLISECONDS);
				if (messages != null) {
//					try {
					consumeRecords(messages);
//					} catch (Exception e ){
//						LOG.error("Caught exception while consuming records. Pushing all records back to the deque for retry.");
//					}

				} else {
					LOG.trace("Did not receive any records (waited {} {}} ) for repository {}. Flushing all records in batch.", timeoutCommitMs,
						TimeUnit.MILLISECONDS, repository);
					flushUpdates();
				}
			} catch (InterruptedException e) {
				LOG.info("Thread was interrupted. Shutting down processor");
				break;
			}
		}
		Thread.interrupted();
		shutdown();
	}

	private void shutdown() {
		// commit any records left before shutdown
		// TODO how much can we wait for this shutdown to complete?
		LOG.info("Commiting any records left before shutdown");
		while (sinkRecords.peek() != null) {
			consumeRecords(sinkRecords.peek());
			sinkRecords.poll();
		}
		// final flush after all messages have been batched
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
			LOG.debug("Cleared {} failed records.", failedRecords.size());
			failedRecords.clear();
			if (operator.execAndHandleError(this) == null) {
				LOG.warn("Flushing failed to execute the update");
				if (!errorHandler.isTolerable()) {
					LOG.error("Errors are not tolerated in ErrorTolerance.NONE");
					throw new RuntimeException("Flushing failed to execute the update");
					// if retrying doesn't solve the problem
				} else {
					LOG.warn("ERROR is TOLERATED the operation continues...");
				}
			}
		}
	}

	@Override
	public Object call() throws RetriableException {
		long start = System.currentTimeMillis();
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			int recordsInCurrentBatch = recordsBatch.size();
			LOG.trace(
				"Transaction started, batch size: {} , records in current batch: {}", batchSize, recordsInCurrentBatch);
			for (SinkRecord record : recordsBatch) {
				if (!failedRecords.contains(record)) {
					handleRecord(record, connection);
				}
			}
			connection.commit();
			LOG.trace("Transaction commited, Batch size: {} , Records in current batch: {}", batchSize, recordsInCurrentBatch);
			LOG.debug("Cleared {} failed records.", failedRecords.size());
			recordsBatch.clear();
			failedRecords.clear();
			if (LOG.isTraceEnabled()) {
				long finish = System.currentTimeMillis();
				LOG.trace("Finished batch processing for {} ms", finish - start);
			}
			return SUCCESSES;
		} catch (Exception e) {
			throw new RetriableException(e);
		}
	}

	void handleRecord(SinkRecord record, RepositoryConnection connection) throws RetriableException {
		long start = System.currentTimeMillis();
		try {
			LOG.trace("Executing {} operation......", transactionType.toString().toLowerCase());
			recordHandler.handle(record, connection);
		} catch (IOException e) {
			throw new RetriableException(e.getMessage(), e);
		} catch (Exception e) {
			handleFailedRecord(record, e);
		} finally {
			if (LOG.isTraceEnabled()) {
				LOG.trace("Record info: {}", ValueUtil.recordInfo(record));
				long finish = System.currentTimeMillis();
				LOG.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
			}
		}

	}

	void handleFailedRecord(SinkRecord record, Exception e) {
		failedRecords.add(record);
		errorHandler.handleFailingRecord(record, e);
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

	public BlockingDeque<Collection<SinkRecord>> getDeque() {
		return sinkRecords;
	}

	private interface RecordHandler {
		void handle(SinkRecord record, RepositoryConnection connection) throws Exception;
	}
}
