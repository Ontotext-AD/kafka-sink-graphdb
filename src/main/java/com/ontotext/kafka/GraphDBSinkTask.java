package com.ontotext.kafka;

import com.ontotext.kafka.logging.LoggerFactory;
import com.ontotext.kafka.logging.LoggingContext;
import com.ontotext.kafka.processor.SinkRecordsProcessor;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;

import static com.ontotext.kafka.processor.SinkProcessorManager.getRunningProcessor;
import static com.ontotext.kafka.processor.SinkProcessorManager.startNewProcessor;

/**
 * {@link SinkTask} implementation that sends the incoming {@link SinkRecord} messages to a synchronous queue for further processing downstream
 * queue to be processed.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkTask extends SinkTask {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private GraphDBSinkConfig config;


	@Override
	public void start(Map<String, String> properties) {
		this.config = new GraphDBSinkConfig(properties);
		try (LoggingContext ctx = LoggingContext.withContext("connectorName=" + config.getConnectorName())) {
			log.info("Task started");
		}
	}



	@Override
	public void put(Collection<SinkRecord> collection) {
		if (CollectionUtils.isEmpty(collection)) {
			return;
		}
		try (LoggingContext ctx = LoggingContext.withContext("connectorName=" + config.getConnectorName())) {
			SinkRecordsProcessor processor = getRunningProcessor(config.getConnectorName());
			if (processor == null) {
				log.warn("Processor {} has completed. Recreating processor", config.getConnectorName());
				processor = startNewProcessor(config);
			}
			if (processor.shouldBackOff()) {
				log.info("Congestion in processor, backing off");
				context.timeout(config.getBackOffTimeoutMs());
				throw new RetriableException("Congestion in processor, retry later");
			}
			log.trace("Sink task received {} records", collection.size());
			processor.getQueue().add(collection);
		}
	}

	@Override
	public void stop() {
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}


}

