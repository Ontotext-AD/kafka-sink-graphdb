package com.ontotext.kafka.test.framework;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.util.HashMap;
import java.util.Map;

import static com.ontotext.kafka.GraphDBSinkConfig.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class TestSinkConfigBuilder {

	private final Map<String, Object> configProperties = new HashMap<>();

	public TestSinkConfigBuilder() {
		initDefaults();
	}

	private void initDefaults() {
		this.configProperties.put(SERVER_IRI, "localhost");
		this.configProperties.put(REPOSITORY, "repo");

	}


	public TestSinkConfigBuilder batchSize(int batchSize) {
		this.configProperties.put(BATCH_SIZE, batchSize);
		return this;
	}

	public TestSinkConfigBuilder timeoutCommitMs(long ms) {
		this.configProperties.put(BATCH_COMMIT_SCHEDULER, ms);
		return this;
	}

	public TestSinkConfigBuilder transactionType(TransactionType type) {
		this.configProperties.put(TRANSACTION_TYPE, type.toString());
		return this;
	}

	public TestSinkConfigBuilder rdfFormat(String rdfFormat) {
		this.configProperties.put(RDF_FORMAT, rdfFormat);
		return this;
	}

	public TestSinkConfigBuilder templateId(String templateId) {
		this.configProperties.put(TEMPLATE_ID, templateId);
		return this;
	}

	public TestSinkConfigBuilder topicName(String topicName) {
		this.configProperties.put(DLQ_TOPIC_DISPLAY, topicName);
		return this;
	}

	public TestSinkConfigBuilder tolerance(ToleranceType toleranceType) {
		this.configProperties.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, toleranceType.toString().toLowerCase());
		return this;
	}

	public TestSinkConfigBuilder bootstrapServers(String bootstrapServers) {
		this.configProperties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		return this;
	}

	public TestSinkConfigBuilder errorRetryTimeout(long timeout) {
		this.configProperties.put(ERRORS_RETRY_TIMEOUT_CONFIG, timeout);
		return this;
	}

	public TestSinkConfigBuilder errorMaxDelayInMillis(long ms) {
		this.configProperties.put(ERRORS_RETRY_MAX_DELAY_CONFIG, ms);
		return this;
	}

	public TestSinkConfigBuilder prop(String key, Object val) {
		this.configProperties.put(key, val);
		return this;
	}

	public GraphDBSinkConfig build() {
		return new GraphDBSinkConfig(this.configProperties);
	}


}
