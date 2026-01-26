package com.ontotext.kafka;

import com.ontotext.kafka.processor.SinkProcessorManager;
import com.ontotext.kafka.processor.SinkRecordsProcessor;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GraphDBSinkTaskTest {

	@Test
	void test_put() {
		String connectorName = UUID.randomUUID().toString();
		Map<String, String> props = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.connectorName(connectorName)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.buildAsProperties();

		GraphDBSinkConfig config = new GraphDBSinkConfig(props);

		SinkRecordsProcessor processorMock = spy(SinkRecordsProcessor.create(config, config.getConnectorName()));
		try (MockedStatic<SinkRecordsProcessor> mockedStatic = mockStatic(SinkRecordsProcessor.class)) {
			mockedStatic.when(() -> SinkRecordsProcessor.create(any(GraphDBSinkConfig.class), any(String.class))).thenReturn(processorMock);
			LinkedBlockingQueue<Collection<SinkRecord>> queue = new LinkedBlockingQueue<>();
			doReturn(queue).when(processorMock).getQueue();


			GraphDBSinkTask task = new GraphDBSinkTask();
			task.start(props);
			GraphDBSinkConnector connector = new GraphDBSinkConnector();
			connector.start(props);

			SinkRecord record = new SinkRecord("topic", 0, null, "key", null, "value", 1);
			Collection<SinkRecord> collectionRecords = Collections.singletonList(record);

			task.put(collectionRecords);

			assertThat(queue).hasSize(1).contains(collectionRecords);
		}
	}

	@Test
	void test_put_noRunningInstance_startInstanceAndPut() throws InterruptedException {
		String connectorName = UUID.randomUUID().toString();
		Map<String, String> props = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.connectorName(connectorName)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.buildAsProperties();

		GraphDBSinkConfig config = new GraphDBSinkConfig(props);

		SinkRecordsProcessor processorMock = spy(SinkRecordsProcessor.create(config, config.getConnectorName()));
		try (MockedStatic<SinkRecordsProcessor> mockedStatic = mockStatic(SinkRecordsProcessor.class)) {
			mockedStatic.when(() -> SinkRecordsProcessor.create(any(GraphDBSinkConfig.class), any(String.class))).thenReturn(processorMock);
			LinkedBlockingQueue<Collection<SinkRecord>> queue = new LinkedBlockingQueue<>();
			doReturn(queue).when(processorMock).getQueue();


			GraphDBSinkTask task = new GraphDBSinkTask();
			task.start(props);
			GraphDBSinkConnector connector = new GraphDBSinkConnector();
			connector.start(props);
			Thread.sleep(1000);


			SinkProcessorManager.stopProcessor(connectorName);

			Thread.sleep(1000);


			SinkRecord record = new SinkRecord("topic", 0, null, "key", null, "value", 1);
			Collection<SinkRecord> collectionRecords = Collections.singletonList(record);

			assertThat(SinkProcessorManager.getRunningProcessor(connectorName)).isNull();
			task.put(collectionRecords);

			assertThat(SinkProcessorManager.getRunningProcessor(connectorName)).isNotNull();

			assertThat(queue).hasSize(1).contains(collectionRecords);
		}
	}


	@Test
	void test_put_processorCongestion_throwRetriableException() throws InterruptedException {
		String connectorName = UUID.randomUUID().toString();
		Map<String, String> props = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.connectorName(connectorName)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.buildAsProperties();

		GraphDBSinkConfig config = new GraphDBSinkConfig(props);
		SinkRecord record = new SinkRecord("topic", 0, null, "key", null, "value", 1);
		Collection<SinkRecord> collectionRecords = Collections.singletonList(record);


		GraphDBSinkTask task = new GraphDBSinkTask();
		task.start(props);

		task.initialize(new WorkerSinkTaskContext(null, null, null));

		GraphDBSinkConnector connector = new GraphDBSinkConnector();
		connector.start(props);
		Thread.sleep(1000);
		task.put(collectionRecords);

		Thread.sleep(2000);

		assertThatCode(() -> task.put(collectionRecords)).isInstanceOf(RetriableException.class).hasMessage("Congestion in processor, retry later");
	}
}
