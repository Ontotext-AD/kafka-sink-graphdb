package com.ontotext.kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SinkRecordsProcessorTest {
	private LinkedList<Reader> streams;
	private LinkedList<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;

	@BeforeEach
	public void setup() {
		streams = new LinkedList<>();
		formats = new LinkedList<>();
		repository = initRepository(streams, formats);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingQueue<>();
	}

	@Test
	@DisplayName("Test shutdown and write unprocessed raw batched message")
	@Timeout(10)
	void testShutdownWriteRawBatchedMessage() throws InterruptedException, IOException {
		int batch = 5;
		generateSinkRecords(sinkRecords, 1, 15);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		assertTrue(formats.isEmpty());
		assertTrue(streams.isEmpty());
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(1, streams.size());
		assertEquals(15, Rio.parse(streams.get(0), RDFFormat.NQUADS).size());
	}

	@Test
	@DisplayName("Test write batched raw messages")
	@Timeout(10)
	void testWriteRawBatchedMessage() throws InterruptedException, IOException {
		int batch = 2;
		generateSinkRecords(sinkRecords, 4, 15);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		awaitCollectionSizeReached(streams, 4);
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(4, streams.size());
		for (Reader reader : streams) {
			assertEquals(15, Rio.parse(reader, RDFFormat.NQUADS).size());
		}
	}

	@Test
	@DisplayName("Test write batched raw messages with messages left for shutdown processing")
	@Timeout(10)
	void testWriteRawBatchedMessageWithRemaining() throws InterruptedException, IOException {
		int batch = 2;
		generateSinkRecords(sinkRecords, 5, 15);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		awaitCollectionSizeReached(streams, 4);
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(5, streams.size());
		for (Reader reader : streams) {
			assertEquals(15, Rio.parse(reader, RDFFormat.NQUADS).size());
		}
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
			Repository repository, int batchSize) {
		Thread thread = new Thread(
				new SinkRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize));
		thread.setDaemon(true);
		return thread;
	}

	private void generateSinkRecords(Queue<Collection<SinkRecord>> sinkRecords, int recordsSize, int statementsSize) {
		for (int i = 0; i < recordsSize; i++) {
			SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null,
					generateRDFStatements(statementsSize).getBytes(),
					12);
			sinkRecords.add(Collections.singleton(sinkRecord));
		}
	}

	private String generateRDFStatements(int quantity) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < quantity; i++) {
			builder.append("<urn:one")
					.append(i)
					.append("> <urn:two")
					.append(i)
					.append("> <urn:three")
					.append(i)
					.append("> . \n");
		}
		return builder.toString();
	}

	private void awaitEmptyCollection(Collection collection) {
		while (!collection.isEmpty()) {
		}
	}

	private void awaitCollectionSizeReached(Collection collection, int size) {
		while (collection.size() < size) {
		}
	}

	private void awaitProcessorShutdown(Thread processor) throws InterruptedException {
		processor.join();
	}

	private Repository initRepository(List<Reader> streams, List<RDFFormat> formats) {
		return new DummyRepository((in, format) -> {
			streams.add(in);
			formats.add(format);
		});
	}
}