package com.ontotext.kafka.error;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.ontotext.kafka.util.PropertiesUtil;
import com.ontotext.kafka.util.ValueUtil;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Tailored DLQ (Dead Letter Queue) producer featuring a specialized error handler, designed to replace the default mechanism.
 * This custom error handler not only takes charge of error management but also enhances logging capabilities for improved diagnostics.
 */

public class LogErrorHandler implements ErrorHandler {

	private static final Logger LOG = LoggerFactory.getLogger(LogErrorHandler.class);
	public static final String PRODUCER_OVERRIDE_PREFIX = "producer.override.";
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
				LOGGER.warn("An Exception={} occurred in {} running in ToleranceType.NONE configuration", ex,
						ValueUtil.recordInfo(record));
				throw new UpdateExecutionException("Record failed", ex);
			}
			case ALL: {
				if (producer != null) {
					LOG.trace("Returning failed record to Kafka.....");
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
		resolveProducerProperties(properties, props);
		resolvePropertiesFromEnvironment(props);

		props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
		logProperties(props);
		return props;
	}

	private void logProperties(Properties props) {
		StringBuilder sb = new StringBuilder("DLQ Properties:\n");
		props.forEach((key, value) -> sb.append(key).append(" = ").append(value).append("\n"));
		LOGGER.info(sb.toString());
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
				key = key.replaceFirst("^CONNECT_PRODUCER_", "")
						.replaceFirst("^" + CONNECT_ENV_PREFIX, "").replace("_", ".").toLowerCase();
				var entryValue = entry.getValue();
				props.put(key, escapeNewLinesFromString(entryValue));
			}
		}
	}

	private void resolveProducerProperties(Map<String, ?> properties, Properties props) {
		for (Map.Entry<String, ?> entry : properties.entrySet()) {
			var key = entry.getKey();
			if (key.startsWith(PRODUCER_OVERRIDE_PREFIX)) {
				props.put(key.substring(PRODUCER_OVERRIDE_PREFIX.length()), entry.getValue());
			}
		}
		var bootstrapServers = properties.get(BOOTSTRAP_SERVERS_CONFIG);
		if (bootstrapServers != null) {
			props.put(BOOTSTRAP_SERVERS_CONFIG, properties.get(BOOTSTRAP_SERVERS_CONFIG));
		}
	}

	@VisibleForTesting
	public static String escapeNewLinesFromString(String value) {
		return value.replace("\\" + System.lineSeparator(), "");
	}
}
