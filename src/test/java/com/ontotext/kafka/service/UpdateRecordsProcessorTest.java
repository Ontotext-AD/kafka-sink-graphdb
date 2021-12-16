package com.ontotext.kafka.service;

import com.ontotext.kafka.mocks.DummyErrorHandler;
import com.ontotext.kafka.mocks.DummyOperator;
import com.ontotext.kafka.operation.OperationHandler;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.Utils.*;
import static org.junit.jupiter.api.Assertions.*;

public class UpdateRecordsProcessorTest {

	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private DummyErrorHandler errorHandler;
	private OperationHandler operator;

	@BeforeEach
	public void setup() {
		streams = new LinkedBlockingQueue<>();
		formats = new LinkedBlockingQueue<>();
		repository = initRepository(streams, formats);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingQueue<>();
		errorHandler = new DummyErrorHandler();
		operator = new DummyOperator();
	}

	@Test
	@DisplayName("Test should skip record with null key")
	@Timeout(5)
	void testShouldSkipInvalidRecord() throws InterruptedException, IOException {
		int batch = 4;
		generateValidSinkRecords(sinkRecords, 3, 15);
		addInValidSinkRecord(sinkRecords);
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
		assertTrue(errorHandler.hasHandled(NullPointerException.class));
	}

	@Test
	@DisplayName("Test should skip multiple invalid records")
	@Timeout(5)
	void testShouldSkipMultipleInvalidRecords() throws InterruptedException, IOException {
		int batch = 5;
		generateValidSinkRecords(sinkRecords, 2, 15);
		addInValidSinkRecord(sinkRecords);
		generateValidSinkRecords(sinkRecords, 1, 15);
		addInValidSinkRecord(sinkRecords);
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
		assertTrue(errorHandler.hasHandled(NullPointerException.class));
		assertEquals(2, errorHandler.numberOfHandled());
	}

	@Test
	@DisplayName("Test should throw when templateId property is missing")
	@Timeout(5)
	public void testShouldThrowWithNullTemplateIdProperty() {
		Assertions.assertThrows(NullPointerException.class,
			() -> new UpdateRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, 2,
				1234, errorHandler, operator, null));
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
										 Repository repository, int batchSize, long commitTimeout) {
		Thread thread = new Thread(
			new UpdateRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
				commitTimeout, errorHandler, operator, "templateId"));
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

}
