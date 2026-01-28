package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.logging.LoggerFactory;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A singleton instance responsible for managing the lifecycle of all {@link SinkRecordsProcessor} threads.
 * Stores all running processors, and only starts new ones (based on the processor UUID)
 * <p>
 * Starting and stopping a new processor instance are thread-safe operations
 */
public final class SinkProcessorManager {

	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	private static final Map<String, ProcessorEntry> runningProcessors = new ConcurrentHashMap<>();
	private static final Set<String> stoppingProcessors = ConcurrentHashMap.newKeySet();
	private static final Logger log = LoggerFactory.getLogger(SinkProcessorManager.class);
	private static final long WAIT_TIMEOUT = 2000;
	private static final int MAX_NUMBER_TIMES_WAITED_FOR_PROCESSOR_TO_STOP = 4;

	private SinkProcessorManager() {
		throw new IllegalStateException("Utility class");
	}


	public static synchronized SinkRecordsProcessor startNewProcessor(GraphDBSinkConfig config) {
		return startNewProcessor(config, WAIT_TIMEOUT, MAX_NUMBER_TIMES_WAITED_FOR_PROCESSOR_TO_STOP);
	}

	/**
	 * Creates a record process for the provided connector. Only one processor per unique connector can exist, therefore processor instances are cached and used when requested.
	 * If a processor for the same connector is already running, that instance will be returned.
	 * If a processor for the same connector is being stopped, this call will wait for it to stop before creating a new instance. The total wait time is calculated based on the waitTimeout and numTimesToWaitForExistingProcessorToStop
	 * parameters
	 *
	 * @param config                                   The configuration of the newly installed connector
	 * @param waitTimeout                              How much time (ms) to wait before checking if an existing processor for the same connector has terminated
	 * @param numTimesToWaitForExistingProcessorToStop How many times to repeat waiting for an existing process to terminate
	 * @return The records processor instance
	 */
	static SinkRecordsProcessor startNewProcessor(GraphDBSinkConfig config, long waitTimeout, int numTimesToWaitForExistingProcessorToStop) {
		try {
			String name = config.getConnectorName();
			ProcessorEntry entry = runningProcessors.get(name);
			if (entry != null) {
				if (!stoppingProcessors.contains(name)) {
					log.warn("Processor with id {} already started", name);
					return entry.processor;
				}
				waitForProcessorToShutdown(entry, waitTimeout, numTimesToWaitForExistingProcessorToStop);
			}
			log.info("Starting processor for connector {}", name);
			entry = new ProcessorEntry();
			SinkRecordsProcessor processor = SinkRecordsProcessor.create(config, name);
			entry.processor = processor;
			// Put the entry into map before starting the processor to prevent issues that may arise if the processor (for some reason) terminates immediately due to an error
			runningProcessors.put(name, entry);
			entry.future = executorService.submit(processor);
			return processor;
		} finally {
			MDC.remove("connectorName");
		}
	}

	private static void waitForProcessorToShutdown(ProcessorEntry entry, long waitTimeout, int numTimesToWaitForExistingProcessorToStop) {
		synchronized (entry.lock) {
			String name = entry.processor.getName();
			log.info("Waiting for processor {} to stop", name);
			int numberOfTimesWaitedForProcessor = numTimesToWaitForExistingProcessorToStop;
			while (stoppingProcessors.contains(name)) {
				if (numTimesToWaitForExistingProcessorToStop <= 0) {
					throw new RetriableException(
						String.format("Waited %dms for processor %s to stop, but processor is still active. Cannot continue creating this processor", numberOfTimesWaitedForProcessor * waitTimeout, name));
				}
				try {
					Thread.sleep(waitTimeout);
				} catch (InterruptedException e) {
					throw new RetriableException(String.format("Interrupted while waiting for processor %s to stop", name), e);
				}
				numTimesToWaitForExistingProcessorToStop--;
			}
		}

	}

	public static void stopProcessor(String name) {
		startEviction(name, null);
		ProcessorEntry entry = runningProcessors.get(name);
		if (entry != null) {
			log.info("Stopping processor with id {}", name);
			entry.future.cancel(true);
			waitForProcessorToShutdown(entry, WAIT_TIMEOUT, MAX_NUMBER_TIMES_WAITED_FOR_PROCESSOR_TO_STOP);
		} else {
			log.warn("Processor with id {} does not exist, it may have already been stopped", name);
		}
	}

	public static SinkRecordsProcessor getRunningProcessor(String connectorName) {
		if (runningProcessors.containsKey(connectorName)) {
			return runningProcessors.get(connectorName).processor;
		} else {
			log.debug("Processor with id {} does not exist", connectorName);
			return null;
		}
	}

	static void startEviction(String name, Throwable errorThrown) {
		if (errorThrown != null) {
			log.warn("Processor {} is stopping because it threw an exception", name, errorThrown);
		}
		stoppingProcessors.add(name);
	}


	/**
	 * A callback to remove the processor from the cache. This is called from the processor itself when closing, because the manager is not able to predict all cases in which the processor will terminate, thus clear the cache accordingly
	 *
	 * @param name        - the connector name bound to the processor to be evicted
	 * @param errorThrown - if the processor terminated exceptionally
	 */
	static void finishEviction(String name, Throwable errorThrown) {
		if (errorThrown != null) {
			log.warn("Processor {} threw exception", name, errorThrown);
		}
		log.info("Removing processor entry {}", name);
		// First remove the entry from the stopping cache,
		stoppingProcessors.remove(name);
		runningProcessors.remove(name);
	}


	private static class ProcessorEntry {
		public final Object lock = new Object();
		private Future<?> future;
		private SinkRecordsProcessor processor;

	}
}
