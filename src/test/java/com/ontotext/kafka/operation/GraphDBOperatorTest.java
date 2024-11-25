package com.ontotext.kafka.operation;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.service.AddRecordsProcessor;
import com.ontotext.kafka.test.framework.ConnectionMockBuilder;
import com.ontotext.kafka.test.framework.RepositoryMockBuilder;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateSinkRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class GraphDBOperatorTest {

	private static final long INFINITE_RETRY = -1;
	private static final long NO_RETRY = 0;
	private static final long NO_DELAY = 0;

	private static final Map<String, Object> property = new HashMap<>();

	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private GraphDBOperator op;
	private GraphDBSinkConfig config;
	private AddRecordsProcessor processor;
	private RepositoryConnection connection;


	@BeforeEach
	public void setup() {
		config = new TestSinkConfigBuilder()
			.errorMaxDelayInMillis(100L)
			.errorRetryTimeout(-1L)
			.tolerance(ToleranceType.ALL)
			.build();


		Queue<Reader> streams = new LinkedBlockingQueue<>();
		Queue<RDFFormat> formats = new LinkedBlockingQueue<>();
		connection = new ConnectionMockBuilder(null, (in, format) -> {
			streams.add(in);
			formats.add(format);
		}, null).build();
		repository = RepositoryMockBuilder.createDefaultMockedRepository(connection);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingQueue<>();

		processor = spy(new AddRecordsProcessor(sinkRecords, shouldRun, repository, config));

	}

	@Test
	public void should_run_throwingProcessor() {
		// Fail on first try, succeed on second
		doThrow(new RepositoryException("ERROR")).doReturn(connection).when(repository).getConnection();


		op = new GraphDBOperator(config);

		generateSinkRecords(sinkRecords, 1, 15);

		assertThat(op.execAndHandleError(processor)).isNotNull();
	}

	@Test
	public void should_handle_toleranceNone() {
		// Fail on first try, succeed on second
		doThrow(new RepositoryException("ERROR")).doReturn(connection).when(repository).getConnection();

		config = new TestSinkConfigBuilder()
			.errorMaxDelayInMillis(100L)
			.errorRetryTimeout(-1L)
			.tolerance(ToleranceType.NONE)
			.build();

		generateSinkRecords(sinkRecords, 1, 15);

		op = new GraphDBOperator(config);

		assertThat(op.execAndHandleError(processor)).isNull();
	}
//
//	@Test
//	public void should_handle_infiniteRetry() {
//		property.put(TOTAL_RETRY_PERIOD_NAME, INFINITE_RETRY);
//		property.put(SLEEP_NAME, NO_DELAY);
//		generateSinkRecords(sinkRecords, 1, 15);
//
//		op = new GraphDBOperator(property);
//		Operation<Object> o = getThrowingProcessor()
//			.setNumberOfThrows(3);
//
//		Assertions.assertNotNull(op.execAndHandleError(o));
//	}
//
//	@Test
//	public void should_handle_noRetry() {
//		property.put(TOTAL_RETRY_PERIOD_NAME, NO_RETRY);
//		property.put(SLEEP_NAME, NO_DELAY);
//		generateSinkRecords(sinkRecords, 1, 15);
//
//		op = new GraphDBOperator(property);
//		Operation<Object> o = getThrowingProcessor()
//			.setNumberOfThrows(1);
//
//		Assertions.assertNull(op.execAndHandleError(o));
//	}
//
//	@Test
//	public void should_handle_longSleep() {
//		property.put(TOTAL_RETRY_PERIOD_NAME, INFINITE_RETRY);
//		property.put(SLEEP_NAME, 1000L);
//		generateSinkRecords(sinkRecords, 1, 15);
//
//		op = new GraphDBOperator(property);
//		Operation<Object> o = getThrowingProcessor()
//			.setNumberOfThrows(2);
//
//		Assertions.assertNotNull(op.execAndHandleError(o));
//	}


}
