package com.ontotext.kafka;

import com.ontotext.kafka.service.AddRecordsProcessor;
import com.ontotext.kafka.service.ReplaceGraphProcessor;
import com.ontotext.kafka.service.SinkRecordsProcessor;
import com.ontotext.kafka.service.UpdateRecordsProcessor;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.commons.collections4.CollectionUtils;
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
		repository = initializeRepository(repository, config);
		LOG.info("Initializing repository");
		recordProcessor = createProcessor();
		recordProcessorThread = new Thread(recordProcessor);
		shouldRun.set(true);
		recordProcessorThread.start();
		LOG.info("Configuration complete.");


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


	private Repository initializeRepository(Repository repository, GraphDBSinkConfig config) {
		return repository == null ? fetchRepository(config) : repository;
	}

	private static Repository fetchRepository(GraphDBSinkConfig config) {
		String address = config.getServerUrl();
		String repositoryId = config.getRepositoryId();
		GraphDBSinkConfig.AuthenticationType authType = config.getAuthType();
		LOG.trace("Fetching repository {} from {}", repositoryId, address);
		HTTPRepository repository = new HTTPRepository(address, repositoryId);
		switch (authType) {
			case NONE:
				return repository;
			case BASIC:
				if (LOG.isTraceEnabled()) {
					LOG.trace("Initializing repository connection with user {}", config.getAuthBasicUser());
				}
				repository.setUsernameAndPassword(config.getAuthBasicUser(), config.getAuthBasicPassword().value());
				return repository;
			case CUSTOM:
			default: // Any other types which are valid, as per definition, but are not implemented yet
				throw new UnsupportedOperationException(authType + " not supported");
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

