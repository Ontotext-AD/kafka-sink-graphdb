package com.ontotext.kafka.error;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.kafka.connect.storage.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.ontotext.kafka.GraphDBSinkConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME;

public class LogErrorHandler implements ErrorHandler {
	//todo we need to use default or what is set in the config
	public static final String DEFAULT_BROKER_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

	private static final Logger logger = LoggerFactory.getLogger(LogErrorHandler.class);
	private static final FailedRecordProducer producer = new FailedRecordProducer(FailedRecordProducer.getDefaultProperties());

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		String msg = record.toString();
		logWarning("Record failed: " + msg + "\n" + ex.getMessage());
		producer.returnFailed(record);
	}

	public static void logError(String msg, Throwable ex) {
		logger.error(msg, ex);
	}

	public static void logError(String msg) {
		logger.error(msg);
	}

	public static void logWarning(String msg, Throwable ex) {
		logger.warn(msg, ex);
	}

	public static void logWarning(String msg) {
		logger.error(msg);
	}

	public static void logInfo(String msg) {
		logger.info(msg);
	}

	private static class FailedRecordProducer {
		private final Properties properties;

		FailedRecordProducer(Properties properties) {
			this.properties = properties;
		}

		void returnFailed(SinkRecord record) {
			String recordKey = record.key() == null ? "null" : record.key().toString();
			String recordValue = record.value() == null ? "null" : record.value().toString();
			Producer<String, String> producer = new KafkaProducer<>(properties);

			try {
				ProducerRecord<String, String> pr = new ProducerRecord<>(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME, recordKey, recordValue);
				producer.send(pr, (metadata, exception) -> {
					String failedRecordInfo = String.format("Record (key=%s value=%s)\n" +
									" meta(partition=%d, offset=%d)\n" +
									" to kafka topic: %s\n",
							recordKey, recordValue,
							metadata.partition(), metadata.offset(),
							ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME);

					if (exception == null) {
						logInfo("Successfully returned failed record to kafka.\n" + failedRecordInfo);
					} else {
						logError("Returning failed record to kafka: UNSUCCESSFUL.\n" + failedRecordInfo,
								exception);
					}
				});
			} finally {
				producer.flush();
				producer.close();
			}
		}

		static Properties getDefaultProperties() {
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BROKER_BOOTSTRAP_SERVERS);
			props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			StringConverter a;
			return props;
		}
	}
}
