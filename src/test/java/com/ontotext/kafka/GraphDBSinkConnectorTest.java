package com.ontotext.kafka;

import com.ontotext.kafka.processor.SinkProcessorManager;
import com.ontotext.kafka.processor.SinkRecordsProcessor;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class GraphDBSinkConnectorTest {

	@Test
	void test_start() throws InterruptedException {
		String connectorName = UUID.randomUUID().toString();
		Map<String, String> props = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.connectorName(connectorName)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.buildAsProperties();

		assertThat(SinkProcessorManager.getRunningProcessor(connectorName)).isNull();
		GraphDBSinkConnector connector = new GraphDBSinkConnector();
		connector.start(props);
		Thread.sleep(1000);
		SinkRecordsProcessor processor = SinkProcessorManager.getRunningProcessor(connectorName);
		assertThat(processor).isNotNull();
		assertThat(processor.isRunning()).isTrue();


	}

	@Test
	void test_start_stop() throws InterruptedException {
		String connectorName = UUID.randomUUID().toString();
		Map<String, String> props = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.connectorName(connectorName)
			.timeoutCommitMs(1000) // Need a small timeout value to force commits
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.buildAsProperties();

		GraphDBSinkConnector connector = new GraphDBSinkConnector();
		connector.start(props);
		Thread.sleep(1000);
		SinkRecordsProcessor processor = SinkProcessorManager.getRunningProcessor(connectorName);
		assertThat(processor).isNotNull();
		assertThat(processor.isRunning()).isTrue();
		connector.stop();
		Thread.sleep(1000);
		assertThat(SinkProcessorManager.getRunningProcessor(connectorName)).isNull();
		assertThat(processor.isRunning()).isFalse();

	}
}
