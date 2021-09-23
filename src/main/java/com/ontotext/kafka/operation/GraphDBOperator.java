package com.ontotext.kafka.operation;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.utils.SystemTime;
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

import static com.ontotext.kafka.util.PropertiesUtil.getTolerance;

public class GraphDBOperator extends RetryWithToleranceOperator implements OperationHandler {

	public static final long DEFAULT_CONNECTION_RETRY_DEFERRED_TIME = 100L;
	public static final int DEFAULT_CONNECTION_NUMBER_OF_RETRIES = 10;

	private static final Logger LOGGER = LoggerFactory.getLogger(GraphDBOperator.class);
	private static final ErrorHandlingMetrics METRICS = new GraphDBErrorHandlingMetrics();

	@VisibleForTesting
	public GraphDBOperator() {
		super(DEFAULT_CONNECTION_RETRY_DEFERRED_TIME, DEFAULT_CONNECTION_NUMBER_OF_RETRIES * DEFAULT_CONNECTION_RETRY_DEFERRED_TIME,
			ToleranceType.ALL, new SystemTime());
		metrics(METRICS);
	}

	public GraphDBOperator(Map<String, ?> properties) {
		super((Long) properties.get(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG),
			(Long) properties.get(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG), getTolerance(properties), new SystemTime());
		metrics(METRICS);
	}

	@Override
	public <E> E execAndRetry(Operation<E> operation) {
		try {
			return super.execAndRetry(operation);
		} catch (UpdateExecutionException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.warn("Unexpected exception while executing operation: {}", operation, e);
			return null;
		}
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
