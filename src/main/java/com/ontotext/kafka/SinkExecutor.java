package com.ontotext.kafka;

import com.ontotext.kafka.processor.SinkRecordsProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A singleton instance responsible for managing the lifecycle of all {@link SinkRecordsProcessor} threads.
 * Stores all running processors, and only starts new ones (based on the processor UUID)
 * <p>
 * Starting and stopping a new processor instance are thread-safe operations
 */
public final class SinkExecutor {

    private static final SinkExecutor INSTANCE = new SinkExecutor();
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<UUID, Future<?>> runningProcessors = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(SinkExecutor.class);

    private SinkExecutor() {
	}

    public static SinkExecutor getInstance() {
        return INSTANCE;
    }

    public synchronized void startNewProcessor(SinkRecordsProcessor processor) {
        UUID processorId = processor.getId();
        if (runningProcessors.containsKey(processorId)) {
            LOG.info("Processor with id {} already started", processorId);
            return;
        }
		runningProcessors.put(processorId, executorService.submit(processor));
	}

	public synchronized void stopProcessor(SinkRecordsProcessor processor) {
        UUID processorId = processor.getId();
        if (runningProcessors.containsKey(processorId)) {
            LOG.info("Stopping processor with id {}", processorId);
            Future<?> processorFuture = runningProcessors.remove(processorId);
            // Interrupt the processor
            processorFuture.cancel(true);
        } else {
            LOG.info("Processor with id {} does not exist, it may have already been stopped", processorId);
        }
    }
}
