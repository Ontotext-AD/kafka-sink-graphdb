package com.ontotext.kafka.util;

import org.apache.kafka.common.config.Config;

import java.util.Map;

public interface ConfigValidator {

	void validate(Config config, Map<String, String> connectorConfigs);
}
