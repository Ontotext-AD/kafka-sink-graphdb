package com.ontotext.kafka.operation;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.ontotext.kafka.util.PropertiesUtil.getTolerance;

public class GraphDBOperator extends RetryWithToleranceOperator implements OperationHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(GraphDBOperator.class);
	private static final ErrorHandlingMetrics METRICS = new GraphDBErrorHandlingMetrics();

	private final long errorRetryTimeout;
	private final long errorMaxDelayInMillis;
	private final ToleranceType tolerance;

	public GraphDBOperator(Map<String, ?> properties) {
		super((Long) properties.get(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG),
			(Long) properties.get(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG), getTolerance(properties), new SystemTime(), METRICS);
		errorRetryTimeout = (Long) properties.get(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG);
		errorMaxDelayInMillis = (Long) properties.get(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG);
		tolerance = getTolerance(properties);
	}

	@Override
	public <E> E execAndHandleError(Operation<E> operation) {
		try {
			return executeAndRetry(operation);
		} catch (UpdateExecutionException e) {
			LOGGER.warn("Caught an Update Exception while executing operation: {}", operation, e);
			if (tolerance == ToleranceType.NONE) {
				LOGGER.error("Errors are not tolerated in ErrorTolerance.NONE");
				throw e;
			} else {
				return null;
			}
		} catch (Exception e) {
			LOGGER.warn("Unexpected exception while executing operation: {}", operation, e);
			return null;
		}
	}

	private <V> V executeAndRetry(Operation<V> operation) throws Exception {
		int attempt = 0;
		long startTime = new SystemTime().milliseconds();
		long deadline = startTime + errorRetryTimeout;
		do {
			try {
				attempt++;
				return operation.call();
			} catch (RetriableException e) {
				LOGGER.trace("Caught a retryable exception while executing {} operation with {}", operation, this.getClass());
				if (checkRetry(startTime)) {
					backoff(attempt, deadline);
					if (Thread.currentThread().isInterrupted()) {
						LOGGER.trace("Thread was interrupted.");
						return null;
					}
				} else {
					LOGGER.warn("{} is out of retries", this.getClass().getSimpleName());
					LOGGER.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
					return null;
				}
			}
		} while (true);
	}

	private boolean checkRetry(long startTime) {
		if (errorRetryTimeout == -1) {
			return true;
		}
		return (new SystemTime().milliseconds() - startTime) < errorRetryTimeout;
	}

	private void backoff(int attempt, long deadline) {
		SystemTime now = new SystemTime();
		long delay = 1 + new Random().nextInt((int) RETRIES_DELAY_MIN_MS) +
			addDelayPerAttempt(attempt);

		if (delay + now.milliseconds() > deadline &&
			errorRetryTimeout > 0) {
			delay = Math.abs(deadline - now.milliseconds());
		}
		LOGGER.debug("Sleeping for {} millis", delay);
		now.sleep(delay);
	}

	private long addDelayPerAttempt(int attempt) {
		if (errorMaxDelayInMillis == 0 || errorRetryTimeout == 0) {
			return 0;
		}
		long expectedRetries = errorRetryTimeout / errorMaxDelayInMillis;
		if (attempt >= expectedRetries || errorRetryTimeout == -1) {
			return Math.abs(errorMaxDelayInMillis - RETRIES_DELAY_MIN_MS);
		}
		return RETRIES_DELAY_MIN_MS + errorMaxDelayInMillis * attempt / expectedRetries;
	}

	private static class GraphDBErrorHandlingMetrics extends ErrorHandlingMetrics {
		private static final StandaloneConfig WORKER_CONFIG = new StandaloneConfig(getBasicProperties());
		private static int taskId;

		//kafka version 2.8
		GraphDBErrorHandlingMetrics() {
			super(new ConnectorTaskId("GraphDB-connector", ++taskId),
				new ConnectMetrics("GraphDB-worker", WORKER_CONFIG,
					SystemTime.SYSTEM, "GraphDB-cluster-id"));
		}

		@Override
		public void recordFailure() {
			LOGGER.warn("Caught a retryable exception");
		}

		@Override
		public void recordRetry() {
			LOGGER.info("Retrying operation...");
		}

		private static Map<String, String> getBasicProperties() {
			Map<String, String> props = new HashMap<>();
			props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
			props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
			props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/connect.offsets");
			return props;
		}
	}
}
