package com.ontotext.kafka.error;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.ontotext.kafka.util.PropertiesUtil.*;

public class LogErrorHandler implements ErrorHandler {

	static final Logger LOGGER = LoggerFactory.getLogger(LogErrorHandler.class);
	private static final FailedProducer PRODUCER = new FailedRecordProducer(getProperties());

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		String msg = record.toString();
		LOGGER.warn("Record failed: " + msg, ex);
		PRODUCER.returnFailed(record);
	}

	private static Properties getProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getFromPropertyOrDefault(BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
		props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return props;
	}
}
