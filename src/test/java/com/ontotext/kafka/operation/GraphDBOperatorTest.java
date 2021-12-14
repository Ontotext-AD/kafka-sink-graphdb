package com.ontotext.kafka.operation;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.mocks.DummyErrorHandler;
import com.ontotext.kafka.mocks.DummyOperator;
import com.ontotext.kafka.mocks.ThrowingProcessor;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.Utils.generateSinkRecords;
import static com.ontotext.kafka.Utils.initRepository;

public class GraphDBOperatorTest {

	private static final String TOTAL_RETRY_PERIOD_NAME = ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG;
	private static final String SLEEP_NAME = ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG;
	private static final String TOLERANCE_NAME = ConnectorConfig.ERRORS_TOLERANCE_CONFIG;

	private static final long INFINITE_RETRY = -1;
	private static final long NO_RETRY = 0;
	private static final long NO_DELAY = 0;

	private static final Map<String, Object> property = new HashMap<>();

	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private ErrorHandler errorHandler;
	private GraphDBOperator op;


	@BeforeEach
	public void setup() {
		property.put(TOTAL_RETRY_PERIOD_NAME, 1000L);
		property.put(SLEEP_NAME, 100L);
		property.put(TOLERANCE_NAME, "all");

		streams = new LinkedBlockingQueue<>();
		formats = new LinkedBlockingQueue<>();
		repository = initRepository(streams, formats);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingQueue<>();
		errorHandler = new DummyErrorHandler();
	}

	@Test
	public void should_initialize_with_DummyProperty() {
		op = new GraphDBOperator(DummyOperator.getProperty());

		Assertions.assertNotNull(op);
	}

	@Test
	public void should_initialize_testingProcessor() {
		int batch = 4;
		op = new GraphDBOperator(DummyOperator.getProperty());
		Thread recordsProcessor = createProcessorThread(sinkRecords, shouldRun, repository, batch, 500);

		Assertions.assertNotNull(recordsProcessor);
	}

	@Test
	public void should_run_throwingProcessor() {
		int batch = 4;
		op = new GraphDBOperator(property);
		generateSinkRecords(sinkRecords, 1, 15);
		Operation<Object> o = new ThrowingProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batch,
			500, errorHandler, op)
			.setNumberOfThrows(1);

		Assertions.assertNotNull(op.execAndHandleError(o));
	}

	@Test
	public void should_handle_toleranceNone() {
		int batch = 4;
		property.put(TOTAL_RETRY_PERIOD_NAME, NO_RETRY);
		property.put(SLEEP_NAME, NO_DELAY);
		property.put(TOLERANCE_NAME, "none");
		generateSinkRecords(sinkRecords, 1, 15);

		op = new GraphDBOperator(property);
		Operation<Object> o = new ThrowingProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batch,
			500, errorHandler, op)
			.setNumberOfThrows(10);

		Assertions.assertNull(op.execAndHandleError(o));
	}

	@Test
	public void should_handle_infiniteRetry() {
		int batch = 4;
		property.put(TOTAL_RETRY_PERIOD_NAME, INFINITE_RETRY);
		property.put(SLEEP_NAME, NO_DELAY);
		generateSinkRecords(sinkRecords, 1, 15);

		op = new GraphDBOperator(property);
		Operation<Object> o = new ThrowingProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batch,
			500, errorHandler, op)
			.setNumberOfThrows(3);

		Assertions.assertNotNull(op.execAndHandleError(o));
	}

	@Test
	public void should_handle_longSleep() {
		int batch = 4;
		property.put(TOTAL_RETRY_PERIOD_NAME, INFINITE_RETRY);
		property.put(SLEEP_NAME, 1000L);
		generateSinkRecords(sinkRecords, 1, 15);

		op = new GraphDBOperator(property);
		Operation<Object> o = new ThrowingProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batch,
			500, errorHandler, op)
			.setNumberOfThrows(2);

		Assertions.assertNotNull(op.execAndHandleError(o));
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
										 Repository repository, int batchSize, long commitTimeout) {
		Thread thread = new Thread(
			new ThrowingProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS, batchSize,
				commitTimeout, errorHandler, op));

		thread.setDaemon(true);
		return thread;
	}

}
