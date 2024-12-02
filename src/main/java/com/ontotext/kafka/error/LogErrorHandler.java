package com.ontotext.kafka.error;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Tailored DLQ (Dead Letter Queue) producer featuring a specialized error handler, designed to replace the default mechanism.
 * This custom error handler not only takes charge of error management but also enhances logging capabilities for improved diagnostics.
 */

public class LogErrorHandler {

	private static final Logger LOG = LoggerFactory.getLogger(LogErrorHandler.class);
	public static final String PRODUCER_OVERRIDE_PREFIX = "producer.override.";
	public static final String CONNECT_ENV_PREFIX = "CONNECT_";

	private final ToleranceType tolerance;
	private final KafkaRecordProducer producer;

	public LogErrorHandler(GraphDBSinkConfig config) {
		this.tolerance = config.getTolerance();
		this.producer = createProducer(config);
	}

	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		LOG.warn("Record failed: {}", ValueUtil.recordInfo(record), ex);
		if (producer != null && ex != null && ex.getClass().isAssignableFrom(RetriableException.class)) {
			LOG.trace("Returning failed record to Kafka.....");
			producer.returnFailed(record);
		} else {
			LOG.error("Record update failed with a non-retriable exception, will not return record to Kafka");
		}
	}

	private KafkaRecordProducer createProducer(GraphDBSinkConfig config) {
		String topicName = config.getDlqTopicName();
		if (tolerance.equals(ToleranceType.NONE) || StringUtils.isBlank(topicName)) {
			return null;
		}
		return new KafkaRecordProducer(topicName, getProperties(config));
	}

	Properties getProperties(GraphDBSinkConfig config) {
		Properties props = new Properties();
		resolveProducerProperties(config, props);
		resolvePropertiesFromEnvironment(props);

		props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaRecordProducer.class.getSimpleName());
		logProperties(props);
		return props;
	}

	private void logProperties(Properties props) {
		StringBuilder sb = new StringBuilder("DLQ Properties:\n");
		props.forEach((key, value) -> sb.append(key).append(" = ").append(value).append("\n"));
		LOG.info(sb.toString());
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

	private void resolveProducerProperties(GraphDBSinkConfig config, Properties props) {
		props.putAll(config.originalsWithPrefix(PRODUCER_OVERRIDE_PREFIX));
		List<String> bootstrapServers = config.getBootstrapServers();
		if (CollectionUtils.isNotEmpty(bootstrapServers)) {
			props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		}
	}

	static String escapeNewLinesFromString(String value) {
		return value.replace("\\" + System.lineSeparator(), "");
	}
}
