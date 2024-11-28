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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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

	private final Queue<Collection<SinkRecord>> sinkRecords;
	private final Queue<SinkRecord> recordsBatch;
	private final Repository repository;
	private final AtomicBoolean shouldRun;
	private final RDFFormat format;
	private final Timer commitTimer;
	private final ScheduleCommitter scheduleCommitter;
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

	public SinkRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository, GraphDBSinkConfig config) {
		this.sinkRecords = sinkRecords;
		this.recordsBatch = new ConcurrentLinkedQueue<>();
		this.repository = repository;
		this.shouldRun = shouldRun;
		this.updateQueryBuilder = new StringBuilder();
		this.templateId = config.getTemplateId();
		this.format = config.getRdfFormat();
		this.batchSize = config.getBatchSize();
		this.timeoutCommitMs = config.getTimeoutCommitMs();
		this.errorHandler = new LogErrorHandler(config);
		this.operator = new GraphDBOperator(config);
		this.commitTimer = new Timer(true);
		this.scheduleCommitter = new ScheduleCommitter();
		this.timeoutCommitLock = new ReentrantLock();
		this.failedRecords = new HashSet<>();
		this.transactionType = config.getTransactionType();
		this.recordHandler = getRecordHandler(transactionType);
		// repositoryUrl is used for logging purposes
		String repositoryUrl = (repository instanceof HTTPRepository) ? ((HTTPRepository) repository).getRepositoryURL() : "unknown";
		MDC.put("RepositoryURL", repositoryUrl);


	}

	@Override
	public void run() {
		try {
			commitTimer.schedule(scheduleCommitter, timeoutCommitMs, timeoutCommitMs);
			while (shouldRun.get()) {
				Collection<SinkRecord> messages = sinkRecords.peek();
				if (messages != null) {
					consumeRecords(messages);
					sinkRecords.poll();
				}
			}
			// commit any records left before shutdown
			LOG.info("Commiting any records left before shutdown");
			while (sinkRecords.peek() != null) {
				consumeRecords(sinkRecords.peek());
				sinkRecords.poll();
			}
			// final flush after all messages have been batched
			flushUpdates();
		} finally {
			// stop the scheduled committer
			scheduleCommitter.cancel();
			commitTimer.cancel();
		}
	}

	public void flushUpdates() {
		try {
			timeoutCommitLock.lock();
			flushRecordUpdates();
		} finally {
			if (timeoutCommitLock.isHeldByCurrentThread()) {
				timeoutCommitLock.unlock();
			}
		}
	}

	private void consumeRecords(Collection<SinkRecord> messages) {
		try {
			timeoutCommitLock.lock();
			for (SinkRecord message : messages) {
				recordsBatch.add(message);
				if (batchSize <= recordsBatch.size()) {
					flushUpdates();
				}
			}
		} finally {
			if (timeoutCommitLock.isHeldByCurrentThread()) {
				timeoutCommitLock.unlock();
			}
		}
	}

	private void flushRecordUpdates() {
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

	public class ScheduleCommitter extends TimerTask {
		@Override
		public void run() {
			flushUpdates();
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


	private interface RecordHandler {
		void handle(SinkRecord record, RepositoryConnection connection) throws Exception;
	}
}
