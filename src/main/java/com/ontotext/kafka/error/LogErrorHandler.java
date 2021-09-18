package com.ontotext.kafka.error;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.ontotext.kafka.util.PropertiesUtil.DEFAULT_BOOTSTRAP_SERVERS;
import static com.ontotext.kafka.util.PropertiesUtil.getFromPropertyOrDefault;

public class LogErrorHandler implements ErrorHandler {

	private static final FailedProducer PRODUCER = new FailedRecordProducer(getProperties());
	private static final Logger LOGGER = LoggerFactory.getLogger(LogErrorHandler.class);

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		LOGGER.warn("Record failed: {}", record, ex);
		PRODUCER.returnFailed(record);
	}

	private static Properties getProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getFromPropertyOrDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS));
		props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return props;
	}
}
