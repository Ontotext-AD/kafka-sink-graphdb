package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SinkProcessorManagerTest {

	@Test
	void test_createProcessor_processorInstanceReturned() throws InterruptedException {
		GraphDBSinkConfig config = TestSinkConfigBuilder.createDefaultConfig();
		SinkRecordsProcessor processor = SinkProcessorManager.startNewProcessor(config);
		assertThat(processor).isNotNull();
		Thread.sleep(1000);
		assertThat(processor.isRunning()).isTrue();
	}

	@Test
	void test_createProcessor_stopProcessor_createNewProcessor_newInstanceReturned() throws InterruptedException {
		GraphDBSinkConfig config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();
		SinkRecordsProcessor processor = SinkProcessorManager.startNewProcessor(config);
		Thread.sleep(1000);
		SinkProcessorManager.stopProcessor(config.getConnectorName());
		SinkRecordsProcessor secondProcessor = SinkProcessorManager.startNewProcessor(config);
		Thread.sleep(1000);
		assertThat(processor).isNotNull();
		assertThat(processor.isRunning()).isFalse();

		assertThat(secondProcessor).isNotNull();
		assertThat(secondProcessor.isRunning()).isTrue();

		assertThat(processor).isNotEqualTo(secondProcessor);

	}

	@Test
	void test_createProcessor_createProcessorAgain_returnCachedInstance() throws InterruptedException {
		GraphDBSinkConfig config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();
		SinkRecordsProcessor processor = SinkProcessorManager.startNewProcessor(config);
		Thread.sleep(1000);
		SinkRecordsProcessor secondProcessor = SinkProcessorManager.startNewProcessor(config);

		assertThat(processor).isNotNull();
		assertThat(processor.isRunning()).isTrue();

		assertThat(secondProcessor).isEqualTo(processor);

	}

	@Test
	void test_createProcessor_instanceCrashes_createProcessorAgain_returnNewInstance() throws InterruptedException {
		GraphDBSinkConfig config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();
		SinkRecordsProcessor processorMock = spy(SinkRecordsProcessor.create(config, config.getConnectorName()));
		SinkRecordsProcessor processor;
		try (MockedStatic<SinkRecordsProcessor> mockedStatic = mockStatic(SinkRecordsProcessor.class)) {


			mockedStatic.when(() -> SinkRecordsProcessor.create(any(GraphDBSinkConfig.class), any(String.class))).thenReturn(processorMock);

			doThrow(new RuntimeException("A")).doCallRealMethod().when(processorMock).pollForMessages();

			processor = SinkProcessorManager.startNewProcessor(config);
		}
		Thread.sleep(1000);
		SinkRecordsProcessor secondProcessor = SinkProcessorManager.startNewProcessor(config);
		assertThat(processor).isNotEqualTo(secondProcessor);
	}

	@Test
	void test_createProcessor_stopProcessor_processorHangsWhileStopping_createProcessorAgain_throwExceptionCannotCreateNewProcessor() throws InterruptedException {
		GraphDBSinkConfig config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();
		SinkRecordsProcessor processorMock = spy(SinkRecordsProcessor.create(config, config.getConnectorName()));
		SinkRecordsProcessor processor;
		try (MockedStatic<SinkRecordsProcessor> mockedStatic = mockStatic(SinkRecordsProcessor.class)) {

			mockedStatic.when(() -> SinkRecordsProcessor.create(any(GraphDBSinkConfig.class), any(String.class))).thenReturn(processorMock);

			processor = SinkProcessorManager.startNewProcessor(config);
			SinkProcessorManager.startEviction(config.getConnectorName(), null);
		}
		Thread.sleep(2000);
		assertThatCode(() -> SinkProcessorManager.startNewProcessor(config, 1000, 1)).isInstanceOf(RetriableException.class).hasMessage(
			String.format("Waited %dms for processor %s to stop, but processor is still active. Cannot continue creating this processor", 1000, config.getConnectorName()));
	}

}
