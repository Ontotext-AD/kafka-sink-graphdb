package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A processor which batches sink records and flushes the updates to a given
 * GraphDB {@link org.eclipse.rdf4j.repository.http.HTTPRepository}.
 * <p>
 * Batches that do not meet the {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_SIZE} are flushed
 * after passing {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_COMMIT_SCHEDULER} threshold.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public abstract class SinkRecordsProcessor implements Runnable, Operation<Object> {

	protected static final Logger LOGGER = LoggerFactory.getLogger(SinkRecordsProcessor.class);
	private static final Object SUCCESSES = new Object();

	protected final Queue<Collection<SinkRecord>> sinkRecords;
	protected final LinkedBlockingQueue<SinkRecord> recordsBatch;
	protected final Repository repository;
	protected final AtomicBoolean shouldRun;
	protected final RDFFormat format;
	protected final Timer commitTimer;
	protected final ScheduleCommitter scheduleCommitter;
	protected final ReentrantLock timeoutCommitLock;
	protected final int batchSize;
	protected final long timeoutCommitMs;
	protected final ErrorHandler errorHandler;
	protected final Set<SinkRecord> failedRecords;
	protected final OperationHandler operator;

	protected SinkRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
								   Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs,
								   ErrorHandler errorHandler, OperationHandler operator) {
		this.recordsBatch = new LinkedBlockingQueue<>();
		this.sinkRecords = sinkRecords;
		this.shouldRun = shouldRun;
		this.repository = repository;
		this.format = format;
		this.commitTimer = new Timer(true);
		this.scheduleCommitter = new ScheduleCommitter();
		this.timeoutCommitLock = new ReentrantLock();
		this.batchSize = batchSize;
		this.timeoutCommitMs = timeoutCommitMs;
		this.errorHandler = errorHandler;
		this.failedRecords = new HashSet<>();
		this.operator = operator;
	}

	@Override
	public void run() {
		try {
			commitTimer.schedule(scheduleCommitter, timeoutCommitMs, timeoutCommitMs);
			while (shouldRun.get()) {
				Collection<SinkRecord> messages = sinkRecords.poll();
				if (messages != null) {
					consumeRecords(messages);
				}
			}
			// commit any records left before shutdown
			while (sinkRecords.peek() != null) {
				consumeRecords(sinkRecords.poll());
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

	protected void consumeRecords(Collection<SinkRecord> messages) {
		for (SinkRecord message : messages) {
			recordsBatch.add(message);
			if (batchSize <= recordsBatch.size()) {
				flushUpdates();
			}
		}
	}

	protected void flushRecordUpdates() {
		if (!recordsBatch.isEmpty()) {
			LOGGER.debug("Cleared {} failed records.", failedRecords.size());
			failedRecords.clear();
			if (operator.execAndHandleError(this) == null) {
				LOGGER.warn("Flushing failed to execute the update");
				if (!errorHandler.isTolerable()) {
					LOGGER.error("Errors are not tolerated in ErrorTolerance.NONE");
					throw new RuntimeException("Flushing failed to execute the update");
					// if retrying doesn't solve the problem
				} else {
					LOGGER.warn("ERROR is TOLERATED the operation continues...");
				}
			}
		}
	}

	@Override
	public Object call() throws RetriableException {
		long start = System.currentTimeMillis();
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			LOGGER.trace("Opened the GraphDB Repository connection.");
			LOGGER.trace("Started batch transaction processing");
			LOGGER.trace("Batch size: {}", batchSize);
			while (!recordsBatch.isEmpty()) {
				SinkRecord record = recordsBatch.remove();
				if (!failedRecords.contains(record)) {
					handleRecord(record, connection);
				}
			}
			connection.commit();
			LOGGER.trace("GraphDB repository connection(transaction) commited");
			failedRecords.clear();
			long finish = System.currentTimeMillis();
			LOGGER.trace("Finished batch processing for {} ms", finish - start);
			return SUCCESSES;
		} catch (Exception e) {
			throw new RetriableException(e);
		}
	}

	protected abstract void handleRecord(SinkRecord record, RepositoryConnection connection) throws RetriableException;

	protected void handleFailedRecord(SinkRecord record, Exception e) {
		failedRecords.add(record);
		errorHandler.handleFailingRecord(record, e);
	}

	public class ScheduleCommitter extends TimerTask {
		@Override
		public void run() {
			flushUpdates();
		}
	}
}
