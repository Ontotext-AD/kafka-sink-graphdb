package com.ontotext.kafka.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
	private final LinkedBlockingQueue<byte[]> recordsBatch;
	private final Repository repository;
	private final AtomicBoolean shouldRun;
	private final RDFFormat format;
	private final int batchSize;

	public SinkRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
			Repository repository, int batchSize) {
		this.recordsBatch = new LinkedBlockingQueue<>();
		this.sinkRecords = sinkRecords;
		this.shouldRun = shouldRun;
		this.repository = repository;
		this.batchSize = batchSize;
		this.format = RDFValueUtil.getRDFFormat();
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
	}

	private void consumeRecords(Collection<SinkRecord> messages) {
		for (SinkRecord message : messages) {
			if (batchSize >= recordsBatch.size()) {
				flushRecords();
			}
			recordsBatch.add(getBytes(message.value()));
		}
	}

	private byte[] getBytes(Object obj) {
		try (var bos = new ByteArrayOutputStream();
				var out = new ObjectOutputStream(bos)) {
			out.writeObject(obj);
			out.flush();
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("WELL.... SHIT");
		}
	}

	private void flushRecords() {
		if (!recordsBatch.isEmpty()) {
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				while (recordsBatch.peek() != null) {
					var stream = new ByteArrayInputStream(recordsBatch.poll());
					connection.add(stream, format);
				}
				connection.commit();
			} catch (IOException e) {
				//todo inject error handler
			}
		}
	}
}
