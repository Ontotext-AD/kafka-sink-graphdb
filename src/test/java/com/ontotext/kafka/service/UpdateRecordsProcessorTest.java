package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.GraphDBOperator;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateRecordsProcessorTest {

	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private final ErrorHandler errorHandler = (r, e) -> {
	};
	private final OperationHandler operator = new GraphDBOperator();

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

	@Test
	@DisplayName("Test should throw when templateId property is missing")
	@Timeout(5)
	public void testShouldThrowWithNullTemplateIdProperty(){
		Assertions.assertThrows(NullPointerException.class,
			()-> new UpdateRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, 2,
					1234, errorHandler, operator));
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
										 Repository repository, int batchSize, long commitTimeout) {
		UpdateRecordsProcessor.setTemplateId("templateId");
		Thread thread = new Thread(
			new UpdateRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
				commitTimeout, errorHandler, operator));
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

	private void awaitEmptyCollection(Collection collection) {
		while (!collection.isEmpty()) {
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
