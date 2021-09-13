package com.ontotext.kafka.error;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.errors.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.ontotext.kafka.util.PropertiesUtil.*;

public class LogErrorHandler extends RetryWithToleranceOperator implements ErrorHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(LogErrorHandler.class);
	private static final FailedProducer PRODUCER = new FailedRecordProducer(getProperties());
	private static final int ATTEMPTS = getFromPropertyOrDefault(CONNECTION_NUMBER_OF_RETRIES, DEFAULT_CONNECTION_NUMBER_OF_RETRIES);
	private static final long DELAY = getFromPropertyOrDefault(CONNECTION_RETRY_DEFERRED_TIME, DEFAULT_CONNECTION_RETRY_DEFERRED_TIME);

	public LogErrorHandler() {
		super(DELAY, ATTEMPTS * DELAY, getTolerance(), SystemTime.SYSTEM);
		super.metrics(new ErrorHandlingMetrics());
	}

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		String msg = record.toString();
		LOGGER.warn("Record failed: " + msg, ex);
		PRODUCER.returnFailed(record);
	}

	@Override
	public <E> E handleRetry(Operation<E> operation) {
		try {
			return super.execAndRetry(operation);
		} catch (Exception e) {
			LOGGER.warn("Retrying err :", e);
			return null;
		}
	}

	private static Properties getProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getFromPropertyOrDefault(BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
		props.put(ProducerConfig.CLIENT_ID_CONFIG, FailedRecordProducer.class.getSimpleName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return props;
	}

	private static ToleranceType getTolerance() {
		String tolerance = getProperty(ERRORS_TOLERANCE);
		if (tolerance == null || "none".equalsIgnoreCase(tolerance)) {
			return ToleranceType.NONE;
		} else if ("all".equalsIgnoreCase(tolerance)) {
			return ToleranceType.ALL;
		} else
			throw new DataException("error: Tolerance can be \"none\" or \"all\". Not supported for - "
					+ tolerance);
	}
}
