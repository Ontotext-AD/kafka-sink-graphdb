package com.ontotext.kafka.error;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.ontotext.kafka.util.PropertiesUtil;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class LogErrorHandler implements ErrorHandler {
	public static final String PRODUCER_OVERRIDE_PROPERTY = "producer.override.";
	public static final String CONNECT_ENV_PREFIX = "CONNECT_";
	private static final Logger LOGGER = LoggerFactory.getLogger(LogErrorHandler.class);

	private final ToleranceType tolerance;
	private final FailedProducer producer;

	public LogErrorHandler(Map<String, ?> properties) {
		this.tolerance = PropertiesUtil.getTolerance(properties);
		this.producer = fetchProducer(properties);
	}

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		LOGGER.warn("Record failed: {}", ValueUtil.recordInfo(record), ex);
		switch (tolerance) {
			case NONE: {
				LOGGER.warn("An Exception={} occurred in {} running in ToleranceType.NONE configuration", ex, ValueUtil.recordInfo(record));
				throw new UpdateExecutionException("Record failed", ex);
			}
			case ALL: {
				if (producer != null) {
					producer.returnFailed(record);
				}
			}
		}
	}

	private FailedRecordProducer fetchProducer(Map<String, ?> properties) {
		String topicName = (String) properties.get(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG);
		if (tolerance.equals(ToleranceType.NONE) || !isValid(topicName)) {
			return null;
		}
		return new FailedRecordProducer(topicName, getProperties(properties));
	}

	private boolean isValid(String topicName) {
		if (topicName == null) {
			return false;
		}
		return !topicName.isBlank();
	}

	private Properties getProperties(Map<String, ?> properties) {
		Properties props = new Properties();
		if (!resolveProducerProperties(properties, props)) {
			resolvePropertiesFromEnvironment(props);
		}
		props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
		return props;
	}

	@Override
	public boolean isTolerable() {
		return tolerance == ToleranceType.ALL;
	}

	private void resolvePropertiesFromEnvironment(Properties props) {
		var envVars = System.getenv();
		for (Map.Entry<String, String> entry : envVars.entrySet()) {
			var key = entry.getKey();
			if (key.startsWith(CONNECT_ENV_PREFIX)) {
				key = key.replaceFirst("^CONNECT_CONSUMER_", "")
					.replaceFirst("^CONNECT_PRODUCER_", "")
					.replaceFirst("^" + CONNECT_ENV_PREFIX, "").replace("_", ".").toLowerCase();
				var entryValue = entry.getValue();
				entryValue = entryValue.replace("\\" + System.getProperty("line.separator", "\n"), "");
				props.put(key, entryValue);
			}
		}
	}

	private boolean resolveProducerProperties(Map<String, ?> properties, Properties props) {
		boolean producerPropertiesResolvedFromPassed = false;
		for (Map.Entry<String, ?> entry : properties.entrySet()) {
			var key = entry.getKey();
			if (key.startsWith(PRODUCER_OVERRIDE_PROPERTY)) {
				producerPropertiesResolvedFromPassed = true;
				props.put(key.substring(PRODUCER_OVERRIDE_PROPERTY.length()), entry.getValue());
			}
		}
		if (producerPropertiesResolvedFromPassed) {
			var bootstrapServers = properties.get(BOOTSTRAP_SERVERS_CONFIG);
			if (StringUtils.isNotEmpty((String) bootstrapServers)) {
				props.put(BOOTSTRAP_SERVERS_CONFIG, properties.get(BOOTSTRAP_SERVERS_CONFIG));
			}
		}
		return producerPropertiesResolvedFromPassed;
	}

	@VisibleForTesting
	public String escapeNewLinesFromString(String value) {
		return value.replace("\\" + System.getProperty("line.separator", "\n"), "");
	}
}
