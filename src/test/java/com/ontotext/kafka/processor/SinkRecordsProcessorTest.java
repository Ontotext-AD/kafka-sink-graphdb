package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.processor.record.handler.RecordHandler;
import com.ontotext.kafka.rdf.repository.MockRepositoryManager;
import com.ontotext.kafka.rdf.repository.RepositoryManager;
import com.ontotext.kafka.test.framework.RepositoryMockBuilder;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateSinkRecord;
import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateSinkRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

public class SinkRecordsProcessorTest {

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
		config = TestSinkConfigBuilder.createDefaultConfig();
	}

	@Test
	@Timeout(5)
	void test_runProcessor_interrupted_exit() throws InterruptedException {
		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
		doThrow(new InterruptedException("Interrupted")).when(processor).pollForMessages();

		processor.run();
		assertThat(Thread.interrupted()).as("After interruption, the processor must clear the interrupt flag").isFalse();
		verify(processor, atLeastOnce()).shutdown();
	}

	@Test
	@Timeout(5)
	void test_runProcessor_interrupted_in_main_loop_exit() throws InterruptedException {
		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
		doAnswer(invocation -> {
			Thread.currentThread().interrupt();
			return null;
		}).when(processor).consumeRecords(any());
		sinkRecords.add(generateSinkRecords(10, 10));
		processor.run();
		assertThat(Thread.interrupted()).as("After interruption, the processor must clear the interrupt flag").isFalse();
		verify(processor, atLeastOnce()).shutdown();
	}


	@Test
	@Timeout(5)
	void test_runProcessor_not_enough_records_for_full_batch_poll_timeout_flush_batch() throws InterruptedException {
		config = new TestSinkConfigBuilder()
			.timeoutCommitMs(10) // some small timeout to not have to wait a long time
			.batchSize(10)
			.build();
		Collection<SinkRecord> records = generateSinkRecords(2, 2);
		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
		final boolean[] stop = {false};

		// Don't stop until the sink records have been consumed, and until the processor has polled on an empty queue at least once (to force the timeout)
		doAnswer(invocation -> {
			if (sinkRecords.isEmpty()) {
				if (stop[0]) {
					return false;
				}
				stop[0] = true;
			}
			return true;
		}).when(processor).shouldRun();

		ArgumentCaptor<Queue<SinkRecord>> captor = ArgumentCaptor.forClass(Queue.class);
		sinkRecords.add(records);

		doNothing().when(processor).flushUpdates(any());
		doNothing().when(processor).shutdown(); // Ignore flush logic in shutdown to make sure that flush logic on timeout is being called

		processor.run();

		assertThat(sinkRecords).as("All records have been consumed").isEmpty();


		verify(processor, atLeastOnce()).shutdown();
		verify(processor, times(1)).flushUpdates(captor.capture());

		Queue<SinkRecord> batch = captor.getValue();
		assertThat(batch).as("Batch must not be empty").isNotEmpty().as("Batch must contain those elements that were consumed").hasSameElementsAs(records);
	}

	@Test
	@Timeout(5)
	void test_runProcessor_consumeRecords_batchFull_flushBatch() throws InterruptedException {
		int batchSize = 10;
		// Fill in the batch twice
		Collection<SinkRecord> firstBatchRecords = generateSinkRecords(batchSize, 20);
		Collection<SinkRecord> secondBatchRecords = generateSinkRecords(batchSize, 20);

		config = new TestSinkConfigBuilder()
			.timeoutCommitMs(10) // some small timeout to not have to wait a long time
			.batchSize(batchSize)
			.build();
		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));


		doAnswer(invocation -> !sinkRecords.isEmpty()).when(processor).shouldRun();

		sinkRecords.add(firstBatchRecords);
		sinkRecords.add(secondBatchRecords);


		List<SinkRecord> consumedRecords = new ArrayList<>();

		doAnswer(invocation -> {
			Queue<SinkRecord> batch = invocation.getArgument(0);
			SinkRecord record;
			while ((record = batch.poll()) != null) {
				consumedRecords.add(record);
			}
			return null;
		}).when(processor).flushUpdates(any());

		doNothing().when(processor).shutdown(); // Ignore flush logic in shutdown to make sure that flush logic on timeout is being called

		processor.run();

		assertThat(sinkRecords).as("All records have been consumed").isEmpty();


		verify(processor, atLeastOnce()).shutdown();
		verify(processor, times(2)).flushUpdates(any());

		assertThat(consumedRecords).hasSize(batchSize * 2);
		assertThat(consumedRecords).containsAll(firstBatchRecords);
		assertThat(consumedRecords).containsAll(secondBatchRecords);
	}


	@Test
	@Timeout(5)
	void test_handleAddRecord_ok() throws InterruptedException, IOException {
		config = new TestSinkConfigBuilder().transactionType(GraphDBSinkConfig.TransactionType.ADD).build();
		try (MockedStatic<RecordHandler> mockedStatic = mockStatic(RecordHandler.class)) {
			RecordHandler handlerMock = (record, connection, config1) -> {
			};

			mockedStatic.when(() -> RecordHandler.getRecordHandler(eq(GraphDBSinkConfig.TransactionType.ADD))).thenReturn(handlerMock);

			processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
			assertThat(processor.handleRecord(generateSinkRecord(2), null)).isNull();

		}
	}

	@Test
	@Timeout(5)
	void test_handleAddRecord_IOException_throwRetriableException() throws InterruptedException, IOException {
		config = new TestSinkConfigBuilder().transactionType(GraphDBSinkConfig.TransactionType.ADD).build();

		try (MockedStatic<RecordHandler> mockedStatic = mockStatic(RecordHandler.class)) {
			RecordHandler handlerMock = (record, connection, config1) -> {
				throw new IOException("IOException");
			};
			mockedStatic.when(() -> RecordHandler.getRecordHandler(eq(GraphDBSinkConfig.TransactionType.ADD))).thenReturn(handlerMock);

			processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
			assertThatCode(() -> processor.handleRecord(generateSinkRecord(2), null)).isInstanceOf(RetriableException.class).hasMessage("IOException");

		}
	}

	@Test
	void test_shutDownProcessor_flushAllRecords() throws InterruptedException {
		int batchSize = 10;
		// Fill in the batch twice
		Collection<SinkRecord> firstBatchRecords = generateSinkRecords(batchSize, 20);
		Collection<SinkRecord> secondBatchRecords = generateSinkRecords(batchSize, 20);

		config = new TestSinkConfigBuilder()
			.timeoutCommitMs(10) // some small timeout to not have to wait a long time
			.batchSize(batchSize)
			.build();
		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));

		sinkRecords.add(firstBatchRecords);
		sinkRecords.add(secondBatchRecords);


		List<SinkRecord> consumedRecords = new ArrayList<>();

		doAnswer(invocation -> {
			Queue<SinkRecord> batch = invocation.getArgument(0);
			SinkRecord record;
			while ((record = batch.poll()) != null) {
				consumedRecords.add(record);
			}
			return null;
		}).when(processor).flushUpdates(any());


		processor.shutdown();

		assertThat(sinkRecords).as("All records have been consumed").isEmpty();
		verify(processor, times(1)).flushUpdates(any());

		assertThat(consumedRecords).hasSize(batchSize * 2);
		assertThat(consumedRecords).containsAll(firstBatchRecords);
		assertThat(consumedRecords).containsAll(secondBatchRecords);
	}

	@Test
	@Timeout(5)
	void test_runProcessor_retriableExceptionOnConsume_backOffSleep_tryAgainAndFlush() throws InterruptedException {

		config = new TestSinkConfigBuilder()
			.timeoutCommitMs(10) // some small timeout to not have to wait a long time
			.backOffRetryTimeoutMs(10)
			.batchSize(1)
			.build();
		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));

		Collection<SinkRecord> records = generateSinkRecords(1, 20);
		sinkRecords.add(records);

		List<SinkRecord> consumedRecords = new ArrayList<>();

		// First pass - get records and try to flush, catch Retriable exception, second pass flush records after backoff wait, then exit
		doReturn(true).doReturn(true).doReturn(false).when(processor).shouldRun();

		// Expect flushUpdates to be called 3 times: first which fails with Retriable exception, second just after backoff sleep, third after the second beacuse poll() returns null messages
		doThrow(new RetriableException("Failed")).doAnswer(invocation -> {
			Queue<SinkRecord> batch = invocation.getArgument(0);
			SinkRecord record;
			while ((record = batch.poll()) != null) {
				consumedRecords.add(record);
			}
			return null;
		}).doNothing().when(processor).flushUpdates(any());

		doNothing().when(processor).shutdown();
		processor.run();
		assertThat(sinkRecords).as("All records have been consumed").isEmpty();
		assertThat(consumedRecords).containsAll(records);
		verify(processor, times(3)).flushUpdates(any());

	}

	@Test
	@Timeout(5)
	void test_doFlush_ok() {

		RepositoryConnection mockConnection = mock(RepositoryConnection.class);
		doReturn(mockConnection).when(repositoryMgr).newConnection();

		doNothing().when(mockConnection).commit();
		doNothing().when(mockConnection).begin();
		Collection<SinkRecord> records = generateSinkRecords(10, 20);
		Queue<SinkRecord> recordBatch = new ConcurrentLinkedQueue<>(records);

		Collection<SinkRecord> consumedRecords = new ArrayList<>();

		RecordHandler handler = (record, connection, config1) -> {
			consumedRecords.add(record);
		};
		try (MockedStatic<RecordHandler> mock = mockStatic(RecordHandler.class)) {
			mock.when(() -> RecordHandler.getRecordHandler(any())).thenReturn(handler);

			processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));

			assertThatCode(() -> processor.doFlush(recordBatch)).doesNotThrowAnyException();


		}
		assertThat(recordBatch).as("All records must have been consumed").isEmpty();
		assertThat(records).containsAll(consumedRecords);

	}


}
