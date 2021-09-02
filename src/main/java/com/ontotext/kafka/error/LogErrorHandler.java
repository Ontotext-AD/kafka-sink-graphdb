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
/*
	public static final int DEFAULT_REQUEST_TIMEOUT = 30_000;
	public static final int DEFAULT_DELIVERY_TIMEOUT = 120_000;
	public static final String DELIVERY_TIMEOUT = "delivery.timeout.ms";
	public static final String DELIVERY_TIMEOUT_DOC = "An upper bound on the time to report success or failure after a call to send() returns.\n" +
			" Should be >= request.timeout.ms + linger.ms.\n" +
			" Default is 120000 (2 minutes)";
	public static final String REQUEST_TIMEOUT = "request.timeout.ms";
	public static final String REQUEST_TIMEOUT_DOC = "It controls the maximum amount of time the client will wait for the response of a request.\n" +
			" If the response is not received before the timeout elapses the client will resend the request if necessary or fail the request if retries are exhausted.\n" +
			" This should be larger than replica.lag.time.max.ms (a broker configuration) to reduce the possibility of message duplication due to unnecessary producer retries.";
	public static final String BOOTSTRAP_BROKER_SERVERS = "bootstrap.servers";
	public static final String BOOTSTRAP_BROKER_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.\n" +
			" The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this\n" +
			" list only impacts the initial hosts used to discover the full set of servers.\n" +
			" This list should be in the form host1:port1,host2:port2,....\n" +
			" Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically),\n" +
			" this list need not contain the full set of servers (you may want more than one, though, in case a server is down).";

*/
/*
	public static final String MAX_REQUEST_PER_CONNECTION = "max.in.flight.requests.per.connection";
	public static final String MAX_REQUEST_PER_CONNECTION_DOC = "If it is greater than 1 there will be a risk " +
			" of messages re-ordering due to retries";
	public static final int DEFAULT_MAX_REQUEST_PER_CONNECTION = 1;

	public static final String LINGER = "linger.ms";
	public static final String LINGER_DOC = "Time to wait if there are messages < bach.size";
	public static final long DEFAULT_LINGER = 0L;
*/
/*
					   .define(MAX_REQUEST_PER_CONNECTION, ConfigDef.Type.INT, DEFAULT_MAX_REQUEST_PER_CONNECTION, ConfigDef.Importance.LOW,
							   MAX_REQUEST_PER_CONNECTION_DOC)
				       .define(DELIVERY_TIMEOUT, ConfigDef.Type.INT, DEFAULT_DELIVERY_TIMEOUT, ConfigDef.Importance.MEDIUM,
							   DELIVERY_TIMEOUT_DOC)
				       .define(LINGER, ConfigDef.Type.INT, DEFAULT_LINGER, ConfigDef.Importance.MEDIUM,
							   LINGER_DOC)
				       .define(REQUEST_TIMEOUT, ConfigDef.Type.INT, DEFAULT_REQUEST_TIMEOUT, ConfigDef.Importance.MEDIUM,
							   REQUEST_TIMEOUT_DOC)
				       .define(BOOTSTRAP_BROKER_SERVERS, ConfigDef.Type.LIST, DEFAULT_BROKER_BOOTSTRAP_SERVERS, ConfigDef.Importance.HIGH,
							   BOOTSTRAP_BROKER_SERVERS_DOC)
*/