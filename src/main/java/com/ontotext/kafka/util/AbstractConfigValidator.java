package com.ontotext.kafka.util;

import com.ontotext.kafka.logging.LoggerFactory;
import com.ontotext.kafka.logging.LoggingContext;
import org.apache.kafka.common.config.Config;
import org.slf4j.Logger;

import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;

public abstract class AbstractConfigValidator implements ConfigValidator {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public final void validate(Config config, Map<String, String> connectorConfigs) {
		try (LoggingContext ctx = LoggingContext.withContext("connectorName=" + connectorConfigs.get(NAME_CONFIG))) {
			doValidate(config, connectorConfigs);
		}

	}

	protected abstract void doValidate(Config config, Map<String, String> connectorConfigs);

	public org.slf4j.Logger getLog() {
		return log;
	}

}
