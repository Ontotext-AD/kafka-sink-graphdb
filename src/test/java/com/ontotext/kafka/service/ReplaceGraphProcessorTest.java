package com.ontotext.kafka.service;

import com.ontotext.kafka.operation.GraphDBOperator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.Reader;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

class ReplaceGraphProcessorTest {
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private Map<String, Reader> contextMap;
	private Map<Reader, RDFFormat> formatMap;
	private GraphDBOperator operator;

//	@BeforeEach
//	void setup() {
//		contextMap = new ConcurrentHashMap<>();
//		formatMap = new ConcurrentHashMap<>();
//		repository = initRepository(contextMap, formatMap);
//		shouldRun = new AtomicBoolean(true);
//		sinkRecords = new LinkedBlockingQueue<>();
//		errorHandler = new DummyErrorHandler();
//		operator = new DummyOperator();
//
//	}
//
//	@Test
//	@DisplayName("Test shutdown and write unprocessed raw batched message")
//	@Timeout(5)
//	void testShutdownWriteRawBatchedMessageAddOneContext() throws InterruptedException, IOException {
//		int batch = 5;
//		generateSinkRecordsContexts(sinkRecords, new String[]{"http://example/"});
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//
//		recordsProcessor.start();
//		awaitEmptyCollection(sinkRecords);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(1, contextMap.size());
//		assertEquals(1, formatMap.size());
//
//		assertEquals(checkRDFStatementsWithContexts(0), Rio.parse(contextMap.get("http://example/"), RDFFormat.NQUADS).toString());
//	}
//
//	@Test
//	@DisplayName("Test shutdown and write unprocessed raw batched message")
//	@Timeout(5)
//	void testShutdownWriteRawBatchedMessageReplaceMoreContexts() throws InterruptedException, IOException {
//		int batch = 5;
//		generateSinkRecordsContexts(sinkRecords, new String[]{"http://example1/", "http://example2/", "http://example1/"});
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//		recordsProcessor.start();
//
//		awaitEmptyCollection(sinkRecords);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(2, contextMap.size());
//		assertEquals(2, formatMap.size());
//
//		assertEquals(checkRDFStatementsWithContexts(2), Rio.parse(contextMap.get("http://example1/"), RDFFormat.NQUADS).toString());
//		assertEquals(checkRDFStatementsWithContexts(1), Rio.parse(contextMap.get("http://example2/"), RDFFormat.NQUADS).toString());
//	}
//
//	@Test
//	@DisplayName("Test write batched raw messages")
//	@Timeout(5)
//	void testWriteRawBatchedMessage() throws InterruptedException, IOException {
//		int batch = 2;
//		generateSinkRecordsContexts(sinkRecords, new String[]{"http://example1/", "http://example2/", "http://example1/", "http://example2/"});
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//		recordsProcessor.start();
//
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(contextMap, 2);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(2, contextMap.size());
//		assertEquals(2, formatMap.size());
//
//		assertEquals(checkRDFStatementsWithContexts(2), Rio.parse(contextMap.get("http://example1/"), RDFFormat.NQUADS).toString());
//		assertEquals(checkRDFStatementsWithContexts(3), Rio.parse(contextMap.get("http://example2/"), RDFFormat.NQUADS).toString());
//	}
//
//	@Test
//	@DisplayName("Test write batched null message")
//	@Timeout(5)
//	void testWriteRawNullMessage() throws InterruptedException, IOException {
//		int batch = 3;
//		generateSinkRecordsContexts(sinkRecords,
//			new String[]{"http://example1/", "http://example2/", "http://example1/",
//				"http://example2/"});
//		String recordKey = "http://example1/";
//		SinkRecord deleteRecord = new SinkRecord("topic", 0, null, recordKey.getBytes(),
//			null, null, 20);
//		sinkRecords.add(Collections.singleton(deleteRecord));
//
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch,
//			5000);
//		recordsProcessor.start();
//
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(contextMap, 1);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(1, contextMap.size());
//		assertEquals(1, formatMap.size());
//
//		assertEquals(checkRDFStatementsWithContexts(3),
//			Rio.parse(contextMap.get("http://example2/"), RDFFormat.NQUADS).toString());
//	}
//
//	@Test
//	@DisplayName("Test write batched raw messages with messages left for shutdown processing")
//	@Timeout(5)
//	void testWriteRawBatchedMessageWithRemaining() throws InterruptedException, IOException {
//		int batch = 2;
//		String[] contexts = constructGraphIri(5);
//		generateSinkRecordsContexts(sinkRecords, contexts);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000);
//		recordsProcessor.start();
//
//		awaitEmptyCollection(sinkRecords);
//		//space only for two batches of size 2
//		awaitCollectionSizeReached(contextMap, 4);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(5, contextMap.size());
//		assertEquals(5, formatMap.size());
//
//		assertContexts(contexts);
//	}
//
//	@Test
//	@DisplayName("Test write raw messages waiting for timeout as not reached threshold of batch")
//	@Timeout(5)
//	void testWriteBatchTimeout() throws InterruptedException, IOException {
//		int batch = 2;
//		String[] contexts = constructGraphIri(3);
//		generateSinkRecordsContexts(sinkRecords, new String[]{"http://example1/", "http://example2/", "http://example3/",
//			"http://example2/", "http://example3/"});
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 20);
//		recordsProcessor.start();
//
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(contextMap, 1);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(3, contextMap.size());
//		assertEquals(3, formatMap.size());
//
//		assertEquals(checkRDFStatementsWithContexts(0), Rio.parse(contextMap.get("http://example1/"), RDFFormat.NQUADS).toString());
//		assertEquals(checkRDFStatementsWithContexts(3), Rio.parse(contextMap.get("http://example2/"), RDFFormat.NQUADS).toString());
//		assertEquals(checkRDFStatementsWithContexts(4), Rio.parse(contextMap.get("http://example3/"), RDFFormat.NQUADS).toString());
//	}
//
//	@Test
//	@DisplayName("Test write raw messages waiting for timeout as not reached threshold after second batch")
//	@Timeout(5)
//	void testWriteBatchTimeoutAfterProperBatching() throws InterruptedException, IOException {
//		int batch = 2;
//		String[] contexts = constructGraphIri(9);
//		generateSinkRecordsContexts(sinkRecords, contexts);
//		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 600);
//		recordsProcessor.start();
//
//		verifyForMilliseconds(() -> contextMap.size() < 9, 500);
//		awaitEmptyCollection(sinkRecords);
//		awaitCollectionSizeReached(contextMap, 9);
//		shouldRun.set(false);
//		awaitProcessorShutdown(recordsProcessor);
//		assertFalse(recordsProcessor.isAlive());
//
//		assertEquals(9, contextMap.size());
//		assertEquals(9, formatMap.size());
//
//		assertContexts(contexts);
//	}
//
//	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
//										 Repository repository, int batchSize, long commitTimeout) {
//		Thread thread = new Thread(
//			new ReplaceGraphProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
//				commitTimeout, errorHandler, operator));
//		thread.setDaemon(true);
//		return thread;
//	}
//
//	private void generateSinkRecordsContexts(Queue<Collection<SinkRecord>> sinkRecords, String[] contexts) {
//		for (int i = 0; i < contexts.length; i++) {
//			SinkRecord sinkRecord = new SinkRecord("topic", 0, null, contexts[i].getBytes(),
//				null, generateRDFStatementsWithContexts(i)
//				.getBytes(), 12);
//			sinkRecords.add(Collections.singleton(sinkRecord));
//		}
//	}
//
//	private String generateRDFStatementsWithContexts(int graphArrayIndex) {
//		return "<urn:one" + graphArrayIndex +
//			"> <urn:two" + graphArrayIndex +
//			"> <urn:three" + graphArrayIndex +
//			"> . ";
//	}
//
//	private String checkRDFStatementsWithContexts(int graphArrayIndex) {
//		return "[(urn:one" + graphArrayIndex +
//			", urn:two" + graphArrayIndex +
//			", urn:three" + graphArrayIndex +
//			")]";
//	}
//
//	private String[] constructGraphIri(int contextsSize) {
//		String[] contexts = new String[contextsSize];
//		for (int i = 0; i < contextsSize; i++) {
//			contexts[i] = "http://example" + (i + 1) + "/";
//		}
//		return contexts;
//	}
//
//	private void assertContexts(String[] contexts) throws IOException {
//		for (int i = 0; i < contexts.length; i++) {
//			assertEquals(checkRDFStatementsWithContexts(i), Rio.parse(contextMap.get(contexts[i]), RDFFormat.NQUADS).toString());
//		}
//	}
//
//	private <K, V> void awaitCollectionSizeReached(Map<K, V> collection, int size) {
//		while (collection.size() < size) {
//		}
//	}
//
//	private Repository initRepository(Map<String, Reader> contextMap, Map<Reader, RDFFormat> formatMap) {
//		return new DummyRepository(contextMap::put, formatMap::put, (ctx) -> {
//			Reader removed = contextMap.remove(ctx);
//			if (removed != null) {
//				formatMap.remove(removed);
//			}
//		});
//	}
}
