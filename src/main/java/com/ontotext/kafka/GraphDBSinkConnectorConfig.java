package com.ontotext.kafka;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class GraphDBSinkConnectorConfig extends AbstractConfig {

	public GraphDBSinkConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
		super(definition, originals);
	}

	public static ConfigDef conf() {
		return new ConfigDef();
	}
}
