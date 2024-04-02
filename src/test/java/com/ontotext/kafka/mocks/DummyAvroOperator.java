package com.ontotext.kafka.mocks;

import com.ontotext.kafka.operation.GraphDBOperator;
import com.ontotext.kafka.util.PropertiesUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.runtime.ConnectorConfig;

public class DummyAvroOperator extends GraphDBOperator {

	public static final Map<String, Object> property = new HashMap<>();

	static {
		property.put(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG, 1000L);
		property.put(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG, 100L);
		property.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
		PropertiesUtil.setProperty("internal.key.converter", "io.confluent.connect.avro.AvroConverter");
		PropertiesUtil.setProperty("internal.value.converter", "io.confluent.connect.avro.AvroConverter");
		PropertiesUtil.setProperty(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, "io.confluent.connect.avro.AvroConverter");
		PropertiesUtil.setProperty(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, "io.confluent.connect.avro.AvroConverter");
	}

	public DummyAvroOperator() {
		super(property);
	}

	public static Map<String, Object> getProperty() {
		return property;
	}
}
