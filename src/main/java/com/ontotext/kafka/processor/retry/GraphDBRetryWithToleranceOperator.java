package com.ontotext.kafka.processor.retry;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * A {@link RetryWithToleranceOperator} which relaxes the Exception tolerance for errors. This is required if an execution must not
 * throw a non-recoverable exception ({@link java.net.ConnectException}, but rather delegate handling the failure scenario to the caller.
 */
public class GraphDBRetryWithToleranceOperator<T> extends RetryWithToleranceOperator<T> {

	private static final ErrorHandlingMetrics METRICS = new GraphDBErrorHandlingMetrics();

	public GraphDBRetryWithToleranceOperator(GraphDBSinkConfig config) {
		super(config.getErrorRetryTimeout(), config.getErrorMaxDelayInMillis(), config.getTolerance(),
			new SystemTime(), METRICS);
	}


	/**
	 * Set {@link Exception} as the tolerable class for {@link org.apache.kafka.connect.runtime.errors.Stage#KAFKA_CONSUME} Stage, thus rely solely on the inner
	 * error handling of the retry operator
	 *
	 * @param operation the operation to be executed.
	 * @param tolerated the class of exceptions which can be tolerated. Ignored and replaced with {@link Exception}
	 * @return
	 */
	@Override
	protected <V> V execAndHandleError(ProcessingContext<T> context, Operation<V> operation, Class<? extends Exception> tolerated) {
		return super.execAndHandleError(context, operation, Exception.class);
	}

	private static class GraphDBErrorHandlingMetrics extends ErrorHandlingMetrics {
		private static final StandaloneConfig WORKER_CONFIG = new StandaloneConfig(getBasicProperties());
		private static int taskId;
		private static final Logger LOG = LoggerFactory.getLogger(GraphDBErrorHandlingMetrics.class);

		//kafka version 2.8
		GraphDBErrorHandlingMetrics() {
			super(new ConnectorTaskId("GraphDB-connector", ++taskId),
				new ConnectMetrics("GraphDB-worker", WORKER_CONFIG,
					SystemTime.SYSTEM, "GraphDB-cluster-id"));
		}

		@Override
		public void recordFailure() {
			LOG.info("Record operation failed");
			super.recordFailure();
		}

		@Override
		public void recordRetry() {
			LOG.info("Retrying operation");
			super.recordRetry();
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
