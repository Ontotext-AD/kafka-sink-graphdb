package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.processor.record.handler.RecordHandler;
import com.ontotext.kafka.rdf.repository.MockRepositoryManager;
import com.ontotext.kafka.rdf.repository.RepositoryManager;
import com.ontotext.kafka.test.framework.RepositoryMockBuilder;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateSinkRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RetryOperatorTest {

	private LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords;
	private RepositoryManager repositoryMgr;
	private SinkRecordsProcessor processor;
	private GraphDBSinkConfig config;
	private HTTPRepository repository;

	@BeforeEach
	public void setup() {
		repository = RepositoryMockBuilder.createDefaultMockedRepository();
		repositoryMgr = MockRepositoryManager.createManagerSpy(repository);
		sinkRecords = spy(new LinkedBlockingQueue<>());
	}


	@Test
	@Timeout(5)
	void test_doFlush_failOnOuterRetry_retryTwice_ok() {

		config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(5000)
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.errorRetryTimeout(-1L)
			.build();

		RepositoryConnection mockConnection = mock(RepositoryConnection.class);
		doReturn(mockConnection).when(repositoryMgr).newConnection();

		doNothing().when(mockConnection).rollback();
		doThrow(new RepositoryException("Fail")).doThrow(new RepositoryException("Failed twice")).doNothing().when(mockConnection).begin();
		doNothing().when(mockConnection).commit();

		Collection<SinkRecord> records = generateSinkRecords(10, 20);
		Queue<SinkRecord> recordBatch = new ConcurrentLinkedQueue<>(records);

		Collection<SinkRecord> consumedRecords = new ArrayList<>();

		RetryOperator retryOperator = spy(new RetryOperator(config));
		doNothing().when(retryOperator).backoff(anyInt(), anyLong());


		RecordHandler handler = (record, connection, config1) -> {
			consumedRecords.add(record);
		};
		try (MockedStatic<RecordHandler> mock = mockStatic(RecordHandler.class)) {
			mock.when(() -> RecordHandler.getRecordHandler(any())).thenReturn(handler);

			processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr, retryOperator));

			assertThatCode(() -> processor.flushUpdates(recordBatch)).doesNotThrowAnyException();
		}
		verify(retryOperator, times(2)).backoff(anyInt(), anyLong());
		assertThat(recordBatch).as("All records must have been consumed").isEmpty();
		assertThat(records).containsAll(consumedRecords);
	}

	@Test
	@Timeout(5)
	void test_doFlush_failOnOuterRetry_retry_eventuallyFailWithRetryException() {


		config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(5000)
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.errorRetryTimeout(3000L) // 3 seconds long test
			.build();

		RepositoryConnection mockConnection = mock(RepositoryConnection.class);
		doReturn(mockConnection).when(repositoryMgr).newConnection();

		doNothing().when(mockConnection).rollback();
		doThrow(new RepositoryException("Fail")).when(mockConnection).begin();
		doNothing().when(mockConnection).commit();

		Collection<SinkRecord> records = generateSinkRecords(10, 20);
		Queue<SinkRecord> recordBatch = new ConcurrentLinkedQueue<>(records);

		Collection<SinkRecord> consumedRecords = new ArrayList<>();

		RetryOperator retryOperator = spy(new RetryOperator(config));
		doNothing().when(retryOperator).backoff(anyInt(), anyLong());


		RecordHandler handler = (record, connection, config1) -> {
			consumedRecords.add(record);
		};
		try (MockedStatic<RecordHandler> mock = mockStatic(RecordHandler.class)) {
			mock.when(() -> RecordHandler.getRecordHandler(any())).thenReturn(handler);

			processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr, retryOperator));

			assertThatCode(() -> processor.flushUpdates(recordBatch)).isInstanceOf(RetriableException.class).hasMessage("Operation failed");
		}
		verify(retryOperator, atLeast(10)).backoff(anyInt(), anyLong());
		assertThat(recordBatch).as("All records must have been consumed").isNotEmpty();
		assertThat(records).containsAll(consumedRecords);
	}


	@Test
	@Timeout(5)
	void test_doFlush_failOnHandleRecord_canTolerateErrors_discardRecord() throws InterruptedException {

		config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(5000)
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();


		RepositoryConnection mockConnection = mock(RepositoryConnection.class);
		doReturn(mockConnection).when(repositoryMgr).newConnection();

		doNothing().when(mockConnection).rollback();
		doNothing().when(mockConnection).begin();
		doNothing().when(mockConnection).commit();

		Collection<SinkRecord> records = generateSinkRecords(10, 20);
		Collection<SinkRecord> failedRecords = generateSinkRecords(5, 20, "failedKey");
		Queue<SinkRecord> recordBatch = new ConcurrentLinkedQueue<>(records);
		recordBatch.addAll(failedRecords);

		Collection<SinkRecord> consumedRecords = new ArrayList<>();

		CountDownLatch mockedLatch = mock(CountDownLatch.class);
		doReturn(true).when(mockedLatch).await(anyLong(), any(TimeUnit.class));

		RetryOperator retryOperator = new RetryOperator(config, mockedLatch, Time.SYSTEM);

		RecordHandler handler = (record, connection, config1) -> {
			if ("failedKey".equals(record.key())) {
				throw new IOException("Could not handle record");
			}
			consumedRecords.add(record);
		};
		try (MockedStatic<RecordHandler> mock = mockStatic(RecordHandler.class)) {
			mock.when(() -> RecordHandler.getRecordHandler(any())).thenReturn(handler);

			processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr, retryOperator));

			assertThatCode(() -> processor.flushUpdates(recordBatch)).doesNotThrowAnyException();
		}
		assertThat(recordBatch).as("All records must have been consumed").isEmpty();
		assertThat(records).containsAll(consumedRecords);
		assertThat(consumedRecords).doesNotContainAnyElementsOf(failedRecords);
	}


}
