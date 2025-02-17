package com.ontotext.kafka.rdf.repository;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryManager {

	private static final Logger LOG = LoggerFactory.getLogger(RepositoryManager.class);
	private final HTTPRepository repository;

	public RepositoryManager(GraphDBSinkConfig config) {
		this(initRepository(config));
	}

	RepositoryManager(HTTPRepository repository) {
		this.repository = repository;
	}

	private static HTTPRepository initRepository(GraphDBSinkConfig config) {
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

	public String getRepositoryURL() {
		return repository.getRepositoryURL();
	}

	public void shutDownRepository() {
		if (repository.isInitialized()) {
			repository.shutDown();
		}
	}

	public RepositoryConnection newConnection() {
		return repository.getConnection();
	}
}
