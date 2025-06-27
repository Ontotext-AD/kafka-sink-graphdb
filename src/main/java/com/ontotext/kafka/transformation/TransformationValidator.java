package com.ontotext.kafka.transformation;

import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Abstract class that is used for validating transformation configs against Sink connector configs.
 */
public abstract class TransformationValidator implements Transformation<SinkRecord> {
	public ConfigValue validateConfig(String transformationName, final Map<String, String> connectorConfigs) {
		return null;
	}
}
