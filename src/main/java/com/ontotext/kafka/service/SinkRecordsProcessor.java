package com.ontotext.kafka.service;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

import com.ontotext.kafka.util.RDFValueUtil;

public class SinkRecordsProcessor implements Runnable {
	private final Queue<Collection<SinkRecord>> sinkRecords;
	private final LinkedBlockingQueue<Reader> recordsBatch;
	private final Repository repository;
	private final AtomicBoolean shouldRun;
	private final RDFFormat format;
	private final int batchSize;

	public SinkRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
			Repository repository, RDFFormat format, int batchSize) {
		this.recordsBatch = new LinkedBlockingQueue<>();
		this.sinkRecords = sinkRecords;
		this.shouldRun = shouldRun;
		this.repository = repository;
		this.format = format;
		this.batchSize = batchSize;
	}

	@Override
	public void run() {
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
		flushRecords();
	}

	private void consumeRecords(Collection<SinkRecord> messages) {
		for (SinkRecord message : messages) {
			if (batchSize <= recordsBatch.size()) {
				flushRecords();
			}
			recordsBatch.add(RDFValueUtil.convertData(message.value()));
		}
		if (batchSize <= recordsBatch.size()) {
			flushRecords();
		}
	}

	private void flushRecords() {
		if (!recordsBatch.isEmpty()) {
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				while (recordsBatch.peek() != null) {
					connection.add(recordsBatch.poll(), format);
				}
				connection.commit();
			} catch (IOException e) {
				throw new RuntimeException(e);
				//todo first add retries
				//todo inject error handler
			}
		}
	}
}