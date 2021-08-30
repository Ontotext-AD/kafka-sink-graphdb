package com.ontotext.kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class AddRecordsProcessorTest {
	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;

	@BeforeEach
	public void setup() {
		streams = new LinkedBlockingQueue<>();
		formats = new LinkedBlockingQueue<>();
		repository = initRepository(streams, formats);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingQueue<>();
	}

	@Test
	@DisplayName("Test shutdown and write unprocessed raw batched message")
	@Timeout(5)
	void testShutdownWriteRawBatchedMessage() throws InterruptedException, IOException {
		int batch = 5;
		generateSinkRecords(sinkRecords, 1, 15);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		assertTrue(formats.isEmpty());
		assertTrue(streams.isEmpty());
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(1, streams.size());
		assertEquals(15, Rio.parse(streams.poll(), RDFFormat.NQUADS).size());
	}

	@Test
	@DisplayName("Test write batched raw messages")
	@Timeout(5)
	void testWriteRawBatchedMessage() throws InterruptedException, IOException {
		int batch = 2;
		generateSinkRecords(sinkRecords, 4, 15);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
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
	@Timeout(5)
	void testWriteRawBatchedMessageWithRemaining() throws InterruptedException, IOException {
		int batch = 2;
		generateSinkRecords(sinkRecords, 5, 15);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		//space only for two batches of size 2
		awaitCollectionSizeReached(streams, 4);
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(5, streams.size());
		for (Reader reader : streams) {
			assertEquals(15, Rio.parse(reader, RDFFormat.NQUADS).size());
		}
	}

	@Test
	@DisplayName("Test write raw messages waiting for timeout as not reached threshold of batch")
	@Timeout(5)
	void testWriteBatchTimeout() throws InterruptedException, IOException {
		int batch = 4;
		generateSinkRecords(sinkRecords, 3, 12);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		awaitCollectionSizeReached(streams, 1);
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(3, streams.size());
		for (Reader reader : streams) {
			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
		}
	}

	@Test
	@DisplayName("Test write raw messages waiting for timeout as not reached threshold after second batch")
	@Timeout(5)
	void testWriteBatchTimeoutAfterProperBatching() throws InterruptedException, IOException {
		int batch = 4;
		generateSinkRecords(sinkRecords, 9, 12);
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 600);
		recordsProcessor.start();
		verifyForMilliseconds(()-> streams.size() < 9, 500);
		awaitEmptyCollection(sinkRecords);
		awaitCollectionSizeReached(streams, 9);
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(9, streams.size());
		for (Reader reader : streams) {
			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
		}
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
			Repository repository, int batchSize, long commitTimeout) {
		Thread thread = new Thread(
				new AddRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
						commitTimeout));
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

	private void verifyForMilliseconds(Supplier<Boolean> supplier, long ms) {
		long timeUntilSchedule = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < timeUntilSchedule) {
			assertTrue(supplier.get());
		}
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

	private Repository initRepository(Queue<Reader> streams, Queue<RDFFormat> formats) {
		return new DummyRepository((in, format) -> {
			streams.add(in);
			formats.add(format);
		});
	}
}