package com.ontotext.kafka.error;

import com.google.common.annotations.VisibleForTesting;
import com.ontotext.kafka.util.PropertiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class FailedRecordProducer implements FailedProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(FailedRecordProducer.class);
	private static final String TOPIC_NAME = PropertiesUtil.getProperty(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG);
	private Properties properties;
	private Producer<String, String> producer;

	FailedRecordProducer(Properties properties) {
		this.properties = properties;
	}

	@VisibleForTesting
	FailedRecordProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	@Override
	public void returnFailed(SinkRecord record) {
		String recordKey = record.key() == null ? "null" : record.key().toString();
		String recordValue = record.value() == null ? "null" : record.value().toString();
		if (producer == null)
			producer = new KafkaProducer<>(properties);

		try {
			ProducerRecord<String, String> pr = new ProducerRecord<>(TOPIC_NAME, recordKey, recordValue);
			producer.send(pr, (metadata, exception) -> {
				String failedRecordInfo = String.format("Record (key=%s value=%s)\n" +
								" meta(partition=%d, offset=%d)\n" +
								" to kafka topic: %s\n",
						recordKey, recordValue,
						metadata == null ? 0 : metadata.partition(), metadata == null ? 0 : metadata.offset(),
						TOPIC_NAME);

				if (exception == null) {
					LOGGER.info("Successfully returned failed record to kafka.\n" + failedRecordInfo);
				} else {
					LOGGER.error("Returning failed record to kafka: UNSUCCESSFUL.\n" + failedRecordInfo,
							exception);
				}
			});
		} finally {
			producer.flush();
			producer.close();
		}
	}

}