package com.ontotext.kafka;

import com.ontotext.kafka.service.AddRecordsProcessor;
import com.ontotext.kafka.service.ReplaceGraphProcessor;
import com.ontotext.kafka.service.SinkRecordsProcessor;
import com.ontotext.kafka.service.UpdateRecordsProcessor;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SinkTask} implementation that sends the incoming {@link SinkRecord} messages to a synchronous queue for further processing downstream
 * queue to be processed.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkTask extends SinkTask {

	private static final Logger LOG = LoggerFactory.getLogger(GraphDBSinkTask.class);
	private Repository repository;
	private final AtomicBoolean shouldRun = new AtomicBoolean(true);
	private final ConcurrentLinkedQueue<Collection<SinkRecord>> sinkRecords = new ConcurrentLinkedQueue<>();
	private GraphDBSinkConfig config;
	private SinkRecordsProcessor recordProcessor;
	private Thread recordProcessorThread;


	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		LOG.info("Starting the GraphDB sink task");
		config = new GraphDBSinkConfig(properties);
		if (repository == null) {
			LOG.info("Initializing repository");
			recordProcessor = createProcessor();
			recordProcessorThread = new Thread(recordProcessor);
			shouldRun.set(true);
			recordProcessorThread.start();
			LOG.info("Configuration complete.");
		}

		// no need to do anything as records are simply added to a concurrent queue
	}


	@Override
	public void put(Collection<SinkRecord> collection) {
		if (CollectionUtils.isEmpty(collection)) {
			return;
		}
		LOG.trace("Sink task received {} records", collection.size());
		sinkRecords.add(collection);
	}

	@Override
	public void stop() {
		try {
			if (repository != null) {
				repository.shutDown();
			}
		} finally {
			repository = null;
			shouldRun.set(false);
			LOG.info("Task stopped.");
		}
	}


	private Repository initializeRepository(Repository repository, Map<String, ?> properties) {
		return repository == null ? fetchRepository(properties) : repository;
	}

	private static Repository fetchRepository(Map<String, ?> properties) {
		String address = (String) properties.get(GraphDBSinkConfig.SERVER_URL);
		String repositoryId = (String) properties.get(GraphDBSinkConfig.REPOSITORY);
		LOG.trace("Fetching repository {} from {}", repositoryId, address);
		var repository = new HTTPRepository(address, repositoryId);
		switch (GraphDBSinkConfig.AuthenticationType.of((String) properties.get(GraphDBSinkConfig.AUTH_TYPE))) {
			case NONE:
				return repository;
			case BASIC:
				if (LOG.isTraceEnabled()) {
					LOG.trace("Initializing repository connection with user {}",
						properties.get(GraphDBSinkConfig.AUTH_BASIC_USER));
				}
				repository.setUsernameAndPassword(
					(String) properties.get(GraphDBSinkConfig.AUTH_BASIC_USER),
					((Password) properties.get(GraphDBSinkConfig.AUTH_BASIC_PASS)).value());
				return repository;
			case CUSTOM:
			default:
				throw new UnsupportedOperationException(properties.get(GraphDBSinkConfig.AUTH_TYPE) + " not supported");
		}
	}

	private SinkRecordsProcessor createProcessor() {
		// This is guarnteed to always be non-null during config initialization
		switch (config.getTransactionType()) {
			case ADD:
				return new AddRecordsProcessor(sinkRecords, shouldRun, repository, config);
			case REPLACE_GRAPH:
				return new ReplaceGraphProcessor(sinkRecords, shouldRun, repository, config);
			case SMART_UPDATE:
				return new UpdateRecordsProcessor(sinkRecords, shouldRun, repository, config);
			default:
				throw new UnsupportedOperationException("Not implemented yet");
		}
	}
}

