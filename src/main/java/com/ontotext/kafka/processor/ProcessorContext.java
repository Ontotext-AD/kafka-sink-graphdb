package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessorContext {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private static final ExecutorService executor = Executors.newCachedThreadPool();
	private final Repository repository;
	private final SinkRecordsProcessor recordProcessor;
	private final LinkedBlockingDeque<Collection<SinkRecord>> sinkRecords;
	private final AtomicBoolean shouldRun;
	private final String connectorName;

	public ProcessorContext(GraphDBSinkConfig config) {
		this.connectorName = config.getConnectorName();
		MDC.put("Connector", connectorName);
		repository = initRepository(config);
		log.info("Initialized GraphDB repository connection: repository {}, GraphDB instance {}", config.getRepositoryId(), config.getServerUrl());
		sinkRecords = new LinkedBlockingDeque<>();
		shouldRun = new AtomicBoolean(true);
		recordProcessor = new SinkRecordsProcessor(sinkRecords, shouldRun, repository, config);
	}

	private Repository initRepository(GraphDBSinkConfig config) {
		String address = config.getServerUrl();
		String repositoryId = config.getRepositoryId();
		GraphDBSinkConfig.AuthenticationType authType = config.getAuthType();
		HTTPRepository repository = new HTTPRepository(address, repositoryId);
		switch (authType) {
			case NONE:
				return repository;
			case BASIC:
				if (log.isTraceEnabled()) {
					log.trace("Initializing repository connection with user {}", config.getAuthBasicUser());
				}
				repository.setUsernameAndPassword(config.getAuthBasicUser(), config.getAuthBasicPassword().value());
				return repository;
			case CUSTOM:
			default: // Any other types which are valid, as per definition, but are not implemented yet
				throw new UnsupportedOperationException(authType + " not supported");
		}
	}

	public void addSinkRecords(Collection<SinkRecord> records) {
		this.sinkRecords.addFirst(records);
	}

	public void shutdown() {
		try {
			if (repository != null) {
				log.info("Shutting down repository connection");
				repository.shutDown();
			}
		} finally {
			this.shouldRun.set(false);
			log.info("Task stopped.");
		}
	}

	public void startProcessor() {
		log.info("Starting processor for connector {}", this.connectorName);
		executor.submit((Runnable) recordProcessor);

	}
}
