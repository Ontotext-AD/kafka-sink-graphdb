package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

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
	protected final Queue<Collection<SinkRecord>> sinkRecords;
	protected final LinkedBlockingQueue<SinkRecord> recordsBatch;
	protected final Queue<Map.Entry<SinkRecord, Throwable>> failedRecords;
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
		failedRecords = new LinkedBlockingQueue<>();
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

	protected void catchMalformedRecords(SinkRecord record, RuntimeException e) {
		Map.Entry<SinkRecord, Throwable> entry = Map.entry(record, e);
		if (!failedRecords.contains(entry)) {
			failedRecords.offer(entry);
		}
	}

	protected void removeFlushedRecords(int flushedRecords) {
		if (flushedRecords > 0)
			for (int i = 0; i < flushedRecords; i++) {
				recordsBatch.poll();
			}
	}

	protected void clearFailed() {
		while (!failedRecords.isEmpty()) {
			Map.Entry<SinkRecord, Throwable> entry = failedRecords.poll();
			SinkRecord record = entry.getKey();
			Throwable ex = entry.getValue();
			errorHandler.handleFailingRecord(record, ex);
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