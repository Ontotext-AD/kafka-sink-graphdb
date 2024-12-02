package com.ontotext.kafka;

import com.ontotext.kafka.processor.SinkRecordsProcessor;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.commons.collections4.CollectionUtils;
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
	private static final Map<GraphDBSinkConfig, SinkRecordsProcessor> processors = new HashMap<>();
	private GraphDBSinkConfig config;
	private SinkRecordsProcessor processor;


	@Override
	public void start(Map<String, String> properties) {
		log.info("Starting the GraphDB sink task for connector {}", config.getConnectorName());
		this.config = new GraphDBSinkConfig(properties);
		this.processor = startProcessor(config);
		log.info("Configuration complete.");
	}

	/**
	 * Creates a new {@link SinkRecordsProcessor} and starts its thread, for a corresponding connector {@link org.apache.kafka.common.config.Config}
	 * If a context has already been created (i.e. from another task initialization for the same connector), return the existing queue, without starting
	 * any new threads.
	 *
	 * @param config - The configuration for which to create the new context
	 * @return ProcessorContext
	 */
	private SinkRecordsProcessor startProcessor(GraphDBSinkConfig config) {
		if (processors.containsKey(config)) {
			return processors.get(config);
		}
		synchronized (processors) {
			if (processors.containsKey(config)) {
				return processors.get(config);
			}
			log.info("Creating a new processor for connector {}", config.getConnectorName());
			SinkRecordsProcessor processor = new SinkRecordsProcessor(config);
			SinkExecutor.getInstance().startNewProcessor(processor);
			return processor;
		}
	}


	@Override
	public void put(Collection<SinkRecord> collection) {
		if (CollectionUtils.isEmpty(collection)) {
			return;
		}
		log.trace("Sink task received {} records", collection.size());
		try {

			this.processor.getDeque().putFirst(collection);
		} catch (InterruptedException e) {
			log.warn("Thread was interrupted, most probably due to an initiated shutdown. ");
			// How would we handle this potential data loss?
			Thread.interrupted();
		}
	}

	@Override
	public void stop() {
		log.trace("Shutting down processor");
		SinkExecutor.getInstance().stopProcessor(processor);
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

}

