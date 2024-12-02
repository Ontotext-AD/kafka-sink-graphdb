package com.ontotext.kafka.processor;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class GraphDBErrorHandlingMetrics extends ErrorHandlingMetrics {
	private static final StandaloneConfig WORKER_CONFIG = new StandaloneConfig(getBasicProperties());
	private static int taskId;
	private static final Logger LOG = LoggerFactory.getLogger(GraphDBErrorHandlingMetrics.class);

	//kafka version 2.8
	GraphDBErrorHandlingMetrics() {
		super(new ConnectorTaskId("GraphDB-connector", ++taskId),
			new ConnectMetrics("GraphDB-worker", WORKER_CONFIG,
				SystemTime.SYSTEM, "GraphDB-cluster-id"));
	}

	@Override
	public void recordFailure() {
		LOG.warn("Record operation failed");
		super.recordFailure();
	}

	@Override
	public void recordRetry() {
		LOG.info("Retrying operation");
		super.recordRetry();
	}

	private static Map<String, String> getBasicProperties() {
		Map<String, String> props = new HashMap<>();
		props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
		props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
		props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/connect.offsets");
		return props;
	}
}
