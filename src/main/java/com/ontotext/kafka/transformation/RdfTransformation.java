package com.ontotext.kafka.transformation;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

/**
 * Abstract class that is used for validating transformation configs against Sink connector configs.
 */
public abstract class RdfTransformation implements Transformation<SinkRecord> {
	public void validateConfig(String transformationName, final Map<String, String> connectorConfigs) throws ConfigException {

	}
}
