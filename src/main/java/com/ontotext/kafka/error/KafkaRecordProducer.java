package com.ontotext.kafka.error;

import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class KafkaRecordProducer {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordProducer.class);
	private final String topicName;
	private final Producer<String, String> producer;

	KafkaRecordProducer(String topicName, Properties properties) {
		this.producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
		this.topicName = topicName;
	}

	KafkaRecordProducer(Producer<String, String> producer) {
		this.producer = producer;
		this.topicName = "test";
	}

	public void returnFailed(SinkRecord record) {
		String recordKey = ValueUtil.convertValueToStringNullable(record.key());
		String recordValue = ValueUtil.convertValueToStringNullable(record.value());
		try {
			ProducerRecord<String, String> pr = new ProducerRecord<>(topicName, recordKey, recordValue);
			producer.send(pr, (metadata, exception) -> {
				if (exception == null) {
					LOG.info("Successfully returned failed record to Kafka. {}", ValueUtil.recordInfo(record));
				} else {
					LOG.error("Returning failed record to kafka: UNSUCCESSFUL. {}", ValueUtil.recordInfo(record),
						exception);
				}
			});
		} finally {
			producer.flush();
		}
	}

}
