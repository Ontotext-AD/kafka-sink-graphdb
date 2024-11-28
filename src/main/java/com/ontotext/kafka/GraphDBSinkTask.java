package com.ontotext.kafka;

import com.ontotext.kafka.processor.SinkRecordsProcessor;
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
		LOG.info("Initialized GraphDB repository connection: repository {}, GraphDB instance {}", config.getRepositoryId(),
			config.getServerUrl());
		recordProcessor = new SinkRecordsProcessor(sinkRecords, shouldRun, repository, config);
		recordProcessorThread = new Thread(recordProcessor);
		shouldRun.set(true);
		recordProcessorThread.start();
		LOG.info("Configuration complete.");
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

}

