package com.ontotext.kafka.util;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VisibleIfRecommender implements ConfigDef.Recommender{
	final String configKey;
    final Object value;

	public static VisibleIfRecommender VisibleIf(String configKey, Object value) {
		return new VisibleIfRecommender(configKey, value);
	}

	private VisibleIfRecommender(String configKey, Object value) {
		this.configKey = configKey;
		this.value = value;
	}

	@Override
	public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
		return Collections.emptyList();
	}

	@Override
	public boolean visible(String name, Map<String, Object> parsedConfig) {
		return parsedConfig.get(this.configKey).toString().equalsIgnoreCase(value.toString());
	}
}
