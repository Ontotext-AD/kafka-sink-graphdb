package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.ontotext.kafka.util.PropertiesUtil.*;

/**
 * A processor which batches sink records and flushes the smart updates to a given
 * GraphDB {@link org.eclipse.rdf4j.repository.http.HTTPRepository}.
 * <p>
 * Batches that do not meet the {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_SIZE} are flushed
 * after passing {@link com.ontotext.kafka.GraphDBSinkConfig#BATCH_COMMIT_SCHEDULER} threshold.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public abstract class SinkRecordsProcessor implements Runnable {

	protected static final Logger LOGGER = LoggerFactory.getLogger(SinkRecordsProcessor.class);
	protected static final int NUMBER_OF_CONNECTION_RETRIES = getFromPropertyOrDefault(CONNECTION_NUMBER_OF_RETRIES, DEFAULT_CONNECTION_NUMBER_OF_RETRIES);
	protected static final long DEFERRED_TIME_BETWEEN_RETRIES = getFromPropertyOrDefault(CONNECTION_RETRY_DEFERRED_TIME, DEFAULT_CONNECTION_RETRY_DEFERRED_TIME);
	protected static final String ERROR_TOLERANCE = getFromPropertyOrDefault(ERRORS_TOLERANCE, DEFAULT_ERRORS_TOLERANCE);

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

	protected SinkRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
								   Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler) {

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
			//final flush after all messages have been batched
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

	protected abstract void flushRecordUpdates();

	public class ScheduleCommitter extends TimerTask {
		@Override
		public void run() {
			flushUpdates();
		}
	}
}