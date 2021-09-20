package com.ontotext.kafka.service;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.lang.System.setProperty;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateRecordsProcessorTest {

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
	@DisplayName("Test should skip records with null key")
	@Timeout(5)
	void testShutdownWriteRawBatchedMessage() throws InterruptedException, IOException {
		int batch = 4;
		generateValidSinkRecords(sinkRecords, 3, 15);
		addInValidSinkRecord(sinkRecords);
		setProperty("graphdb.template.id", "value");
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		assertTrue(formats.isEmpty());
		assertTrue(streams.isEmpty());
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);

		assertFalse(recordsProcessor.isAlive());
		assertEquals(3, streams.size());
		for (Reader reader : streams) {
			assertEquals(15, Rio.parse(reader, RDFFormat.NQUADS).size());
		}
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
										 Repository repository, int batchSize, long commitTimeout) {
		Thread thread = new Thread(
			new UpdateRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
				commitTimeout));
		thread.setDaemon(true);
		return thread;
	}

	private void generateValidSinkRecords(Queue<Collection<SinkRecord>> sinkRecords, int recordsSize, int statementsSize) {
		for (int i = 0; i < recordsSize; i++) {
			SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "key-value", null,
				generateRDFStatements(statementsSize).getBytes(),
				12);
			sinkRecords.add(Collections.singleton(sinkRecord));
		}
	}

	private void addInValidSinkRecord(Queue<Collection<SinkRecord>> sinkRecords) {
		SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null,
			generateRDFStatements(3).getBytes(),
			12);
		sinkRecords.add(Collections.singleton(sinkRecord));
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
