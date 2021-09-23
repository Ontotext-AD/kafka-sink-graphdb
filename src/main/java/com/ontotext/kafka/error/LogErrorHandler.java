package com.ontotext.kafka.error;

import com.ontotext.kafka.util.PropertiesUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class LogErrorHandler implements ErrorHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(LogErrorHandler.class);

	private final ToleranceType tolerance;
	private final FailedProducer producer;

	public LogErrorHandler(Map<String, ?> properties) {
		this.tolerance = PropertiesUtil.getTolerance(properties);
		this.producer = new FailedRecordProducer(getProperties(properties));
	}

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		LOGGER.warn("Record failed: {}", record, ex);
		switch (tolerance) {
			case NONE: {
				LOGGER.warn("An exception={} occurred in record={} running in Tolerance.NONE configuration", ex, record);
				throw new UpdateExecutionException("Record failed", ex);
			}
			case ALL: {
				producer.returnFailed(record);
			}
		}
	}

	private Properties getProperties(Map<String, ?> properties) {
		Properties props = new Properties();
		props.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, properties.get(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG));
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
		return props;
	}
}
