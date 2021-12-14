package com.ontotext.kafka.mocks;

import com.ontotext.kafka.operation.GraphDBOperator;
import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.util.HashMap;
import java.util.Map;

public class DummyOperator extends GraphDBOperator {

	public static final Map<String, Object> property = new HashMap<>();

	static {
		property.put(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG, 1000L);
		property.put(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG, 100L);
		property.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
	}

	public DummyOperator() {
		super(property);
	}

	public static Map<String, Object> getProperty() {
		return property;
	}
}
