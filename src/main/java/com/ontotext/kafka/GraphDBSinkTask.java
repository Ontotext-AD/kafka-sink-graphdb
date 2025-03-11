package com.ontotext.kafka;

import com.ontotext.kafka.processor.SinkRecordsProcessor;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SinkTask} implementation that sends the incoming {@link SinkRecord} messages to a synchronous queue for further processing downstream
 * queue to be processed.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(GraphDBSinkTask.class);

	/**
	 * Use a single processor and queue for all tasks of a single unique connector.
	 */
	private static final Map<String, SinkRecordsProcessor> processors = new HashMap<>();
	private GraphDBSinkConfig config;
	private SinkRecordsProcessor processor;


	@Override
	public void start(Map<String, String> properties) {
		this.config = new GraphDBSinkConfig(properties);
		log.info("Starting the GraphDB sink task for connector {}", config.getConnectorName());
		this.processor = createAndStartProcessor(config);
		log.info("Configuration complete.");
	}

	/**
	 * Creates a new {@link SinkRecordsProcessor} and starts its thread, for a corresponding connector name.
	 * If a context has already been created (i.e. from another task initialization for the same connector), return the existing queue, without starting
	 * any new threads.
	 *
	 * @param config - The configuration for which to create the new context
	 * @return ProcessorContext
	 */
	private SinkRecordsProcessor createAndStartProcessor(GraphDBSinkConfig config) {
		String connectorName = config.getConnectorName();
		if (processors.containsKey(connectorName)) {
			return processors.get(connectorName);
		}
		synchronized (processors) {
			if (processors.containsKey(connectorName)) {
				return processors.get(connectorName);
			}
			log.info("Creating a new processor for connector {}", connectorName);
			SinkRecordsProcessor processor = processors.compute(connectorName, (s, sinkRecordsProcessor) -> new SinkRecordsProcessor(config));
			SinkExecutor.getInstance().startNewProcessor(processor);
			return processor;
		}
	}


	@Override
	public void put(Collection<SinkRecord> collection) {
		if (CollectionUtils.isEmpty(collection)) {
			return;
		}
		if (processor.shouldBackOff()) {
			log.info("Congestion in processor, backing off");
			context.timeout(config.getBackOffTimeoutMs());
			throw new RetriableException("Congestion in processor, retry later");
		}
		log.info("Sink task received {} records", collection.size());
		processor.getQueue().add(collection);
	}

	@Override
	public void stop() {
		log.info("Shutting down processor");
		SinkExecutor.getInstance().stopProcessor(processor);
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

}

