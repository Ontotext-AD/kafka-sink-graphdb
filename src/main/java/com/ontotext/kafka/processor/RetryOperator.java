package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RetryOperator {


	public static final long RETRIES_DELAY_MIN_MS = 300;

	private final GraphDBSinkConfig config;
	private static final Logger log = LoggerFactory.getLogger(RetryOperator.class);
	private final CountDownLatch stopRequestedLatch;
	private final Time time;

	public RetryOperator(GraphDBSinkConfig config) {
		this.config = config;
		this.stopRequestedLatch = new CountDownLatch(1);
		this.time = Time.SYSTEM;
	}

	/**
	 * Attempt to execute an operation. Handles retriable and tolerated exceptions thrown by the operation.
	 *
	 * @param operation the recoverable operation
	 * @throws RetriableException if the operation fails and errors are tolerated
	 * @throws ConnectException   wrapper if any non-tolerated exception was thrown by the operation
	 */
	public void execute(Runnable operation) {
		Exception errorThrown = execAndHandleError(operation);
		if (errorThrown != null) {
			log.error("Operation failed. Underlying exception - {}", errorThrown.getMessage());
			if (config.getTolerance() == ToleranceType.NONE) {
				throw new ConnectException("Error tolerance exceeded.");
			}
			log.warn("Errors are tolerated (tolerance = {}).", config.getTolerance());
			throw new RetriableException("Operation failed", errorThrown);
		}

	}

	/**
	 * Attempt to execute an operation. Handles retriable exceptions raised by the operation.
	 * <p>Retries are allowed if this operation is within the error retry timeout.
	 *
	 * @param operation the operation to be executed.
	 * @return The resulting exception, if the operation failed, or null
	 * @throws Exception rethrow if any non-retriable exception was thrown by the operation
	 */
	private Exception execAndRetry(Runnable operation) throws Exception {
		int attempt = 0;
		long startTime = System.currentTimeMillis();
		long deadline = (config.getErrorRetryTimeout() >= 0) ? startTime + config.getErrorRetryTimeout() : Long.MAX_VALUE;
		while (true) {
			try {
				attempt++;
				operation.run();
				return null;
			} catch (RetriableException e) {
				log.trace("Caught a retriable exception - {}", e.getMessage());
				if (time.milliseconds() < deadline) {
					backoff(attempt, deadline);
				} else {
					log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
					return e;
				}
			}
		}
	}

	/**
	 * Attempt to execute an operation. Handles retriable and tolerated exceptions thrown by the operation.
	 *
	 * @param operation the operation to be executed.
	 * @return The resulting exception, if the operation failed, or null
	 * @throws ConnectException wrapper if any non-tolerated exception was thrown by the operation
	 */
	protected Exception execAndHandleError(Runnable operation) {
		try {
			return execAndRetry(operation);
		} catch (Exception e) {
			if (config.getTolerance() == ToleranceType.NONE) {
				throw new ConnectException("Tolerance exceeded in error handler", e);
			}
			return e;
		}
	}


	/**
	 * Do an exponential backoff bounded by {@link #RETRIES_DELAY_MIN_MS} and runtime configuration (errors.retry.delay.max.ms)
	 *
	 * @param attempt  the number indicating which backoff attempt it is (beginning with 1)
	 * @param deadline the time in milliseconds until when retries can be attempted
	 */
	void backoff(int attempt, long deadline) {
		int numRetry = attempt - 1;
		long delay = RETRIES_DELAY_MIN_MS << numRetry;
		if (delay > config.getErrorMaxDelayInMillis()) {
			delay = ThreadLocalRandom.current().nextLong(config.getErrorMaxDelayInMillis());
		}
		long currentTime = time.milliseconds();
		if (delay + currentTime > deadline) {
			delay = Math.max(0, deadline - currentTime);
		}
		log.debug("Sleeping for up to {} millis", delay);
		try {
			stopRequestedLatch.await(delay, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			return;
		}
	}
}
