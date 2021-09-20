package com.ontotext.kafka.operation;

import com.ontotext.kafka.error.UnToleratedException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.ontotext.kafka.util.PropertiesUtil.*;

public class GraphDBOperator extends RetryWithToleranceOperator implements OperationHandler {

	public static final long DEFAULT_CONNECTION_RETRY_DEFERRED_TIME = 100L;
	public static final int DEFAULT_CONNECTION_NUMBER_OF_RETRIES = 10;

	private static final Logger LOGGER = LoggerFactory.getLogger(GraphDBOperator.class);
	//default needed for tests
	private static final int RETRIES = getFromPropertyOrDefault(CommonClientConfigs.RETRIES_CONFIG, DEFAULT_CONNECTION_NUMBER_OF_RETRIES);
	private static final long DELAY = getFromPropertyOrDefault(ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG, DEFAULT_CONNECTION_RETRY_DEFERRED_TIME);
	private static final ErrorHandlingMetrics METRICS = new GraphDBErrorHandlingMetrics();

	public GraphDBOperator() {
		super(DELAY, RETRIES * DELAY, getTolerance(), new SystemTime());
		metrics(METRICS);
	}

	@Override
	public <E> E execAndRetry(Operation<E> operation) {
		try {
			return super.execAndRetry(operation);
		}catch (UnToleratedException e){
			throw new RuntimeException(e);
		}catch (Exception e) {
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
			props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
			props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
			props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "/tmp/connect.offsets");
			return props;
		}
	}
}
