package com.ontotext.kafka.service;

import com.ontotext.kafka.operation.GraphDBOperator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.Reader;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

class AddRecordsProcessorTest {
	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private GraphDBOperator operator;

//	@BeforeEach
//	public void setup() {
//		streams = new LinkedBlockingQueue<>();
//		formats = new LinkedBlockingQueue<>();
//		repository = initRepository(streams, formats);
//		shouldRun = new AtomicBoolean(true);
//		sinkRecords = new LinkedBlockingQueue<>();
//		errorHandler = new DummyErrorHandler();
//		operator = new DummyOperator();
//	}
//
//	@Test
//	@DisplayName("Test shutdown and write unprocessed raw batched message")
//	@Timeout(5)
//	void testShutdownWriteRawBatchedMessage() throws InterruptedException, IOException {
//		int batch = 5;
//		generateSinkRecords(sinkRecords, 1, 15);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		assertTrue(formats.isEmpty());
//		assertTrue(streams.isEmpty());
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//		assertEquals(1, streams.size());
//		assertEquals(15, Rio.parse(streams.poll(), RDFFormat.NQUADS).size());
//	}
//
//	@Test
//	@DisplayName("Test write batched raw messages")
//	@Timeout(5)
//	void testWriteRawBatchedMessage() throws InterruptedException, IOException {
//		int batch = 2;
//		generateSinkRecords(sinkRecords, 4, 15);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, 4);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//		assertEquals(4, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(15, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test write batched raw messages with messages left for shutdown processing")
//	@Timeout(5)
//	void testWriteRawBatchedMessageWithRemaining() throws InterruptedException, IOException {
//		int batch = 2;
//		generateSinkRecords(sinkRecords, 5, 15);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		//space only for two batches of size 2
//		awaitCollectionSizeReached(streams, 4);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//		assertEquals(5, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(15, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test write raw messages waiting for timeout as not reached threshold of batch")
//	@Timeout(5)
//	void testWriteBatchTimeout() throws InterruptedException, IOException {
//		int batch = 4;
//		generateSinkRecords(sinkRecords, 3, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, 1);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//		assertEquals(3, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test write raw messages waiting for timeout as not reached threshold after second batch")
//	@Timeout(5)
//	void testWriteBatchTimeoutAfterProperBatching() throws InterruptedException, IOException {
//		int batch = 4;
//		generateSinkRecords(sinkRecords, 9, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 600);
//		recordsProcessor.start();
//		verifyForMilliseconds(() -> streams.size() < 9, 500);
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, 9);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//		assertEquals(9, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test malformed records should get handled")
//	@Timeout(5)
//	void testHandleMalformed() throws InterruptedException, IOException {
//		int batch = 4;
//		int expectedSize = 4;
//
//		generateSinkRecords(sinkRecords, 2, 12);
//		addMalformedSinkRecord(sinkRecords);
//		generateSinkRecords(sinkRecords, 2, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, expectedSize);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test RDFParseException should get handled")
//	@Timeout(5)
//	void testHandleRDFParseException() throws InterruptedException, IOException {
//		int batch = 4;
//		int expectedSize = 3;
//
//		repository = initThrowingRepository(streams, formats, new RDFParseException("RDFParseException"));
//		generateSinkRecords(sinkRecords, 4, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, expectedSize);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test multiple RDFParseException should get handled")
//	@Timeout(5)
//	void testHandleMultipleRDFParseException() throws InterruptedException, IOException {
//		int batch = 4;
//		int expectedSize = 1;
//
//		repository = initThrowingRepository(streams, formats, new RDFParseException("RDFParseException"), 3);
//		generateSinkRecords(sinkRecords, 4, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test all records are malformed RDFParseException")
//	@Timeout(5)
//	void testHandleAllRDFParseExceptionRecords() throws InterruptedException {
//		int batch = 4;
//		int expectedSize = 0;
//
//		repository = initThrowingRepository(streams, formats, new RDFParseException("RDFParseException"), 4);
//		generateSinkRecords(sinkRecords, 4, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//	}
//
//	@Test
//	@DisplayName("Test RepositoryException should get handled")
//	@Timeout(5)
//	void testHandleRepositoryException() throws InterruptedException, IOException {
//		int batch = 4;
//		int expectedSize = 3;
//
//		repository = initThrowingRepository(streams, formats, new RepositoryException());
//		generateSinkRecords(sinkRecords, 4, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, expectedSize);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Test
//	@DisplayName("Test IOExceptions should get retried")
//	@Timeout(5)
//	void testHandleIOException() throws InterruptedException, IOException {
//		int batch = 4;
//		int expectedSize = 3;
//
//		repository = initThrowingRepository(streams, formats, new IOException());
//		generateSinkRecords(sinkRecords, 4, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, expectedSize);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	@Disabled // test fails intermittently. Depends on how fast threads complete
//	@Test
//	@DisplayName("Test should throw when out of retries")
//	@Timeout(5)
//	void testThrowTimoutExceptionException() {
//		int batch = 1;
//
//		repository = initThrowingRepository(streams, formats, new IOException(), 320);
//		generateSinkRecords(sinkRecords, 1, 12);
//
//		AddRecordsProcessor processor = new AddRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batch,
//			50, errorHandler, operator);
//		processor.recordsBatch.addAll(sinkRecords.poll());
//
//		Assertions.assertThrows(RuntimeException.class,
//			processor::flushRecordUpdates);
//	}
//
//	@Test
//	@DisplayName("Test multiple IOExceptions should get retried")
//	@Timeout(5)
//	void testHandleMultipleIOException() throws InterruptedException, IOException {
//		int batch = 4;
//		int expectedSize = 1;
//
//		repository = initThrowingRepository(streams, formats, new IOException(), 3);
//		generateSinkRecords(sinkRecords, 4, 12);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 50);
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(streams, expectedSize);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//
//		assertEquals(expectedSize, streams.size());
//		for (Reader reader : streams) {
//			assertEquals(12, Rio.parse(reader, RDFFormat.NQUADS).size());
//		}
//	}
//
//	private void addMalformedSinkRecord(Queue<Collection<SinkRecord>> sinkRecords) {
//		SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null,
//			null,
//			12);
//		sinkRecords.add(Collections.singleton(sinkRecord));
//	}
//
//	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
//										 Repository repository, int batchSize, long commitTimeout) {
//		Thread thread = new Thread(
//			new AddRecordsProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
//				commitTimeout, errorHandler, operator));
//
//		thread.setDaemon(true);
//		return thread;
//	}
//
//	private Repository initThrowingRepository(Queue<Reader> streams, Queue<RDFFormat> formats, Exception exception) {
//		return initThrowingRepository(streams, formats, exception, 1);
//	}
//
//	private Repository initThrowingRepository(Queue<Reader> streams, Queue<RDFFormat> formats, Exception exception, int numberOfThrows) {
//		return new ThrowingRepository(
//			(in, format) -> {
//				streams.add(in);
//				formats.add(format);
//			},
//			exception, numberOfThrows);
//	}

}
