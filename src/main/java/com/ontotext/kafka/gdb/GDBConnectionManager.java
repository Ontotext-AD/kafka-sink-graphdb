package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.kafka.common.config.ConfigException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ontotext.kafka.GraphDBSinkConfig.SERVER_URL;
import static com.ontotext.kafka.tls.HttpClientManager.createHttpClient;

public class GDBConnectionManager {

	private static final Logger LOG = LoggerFactory.getLogger(GDBConnectionManager.class);
	private final HTTPRepository repository;

	/**
	 * Creates the manager given the entire connector configuration
	 *
	 * @param config - the configuration of the Kafka Sink Connector
	 * @throws org.apache.kafka.common.config.ConfigException if a configuration property was not validated
	 */
	public GDBConnectionManager(GdbConnectionConfig config) {
		try {
			this.repository = initRepository(config, createHttpClient(config));
		} catch (Exception e) {
			throw new ConfigException(SERVER_URL, config.getServerUrl(), e.getMessage());
		}

	}

	GDBConnectionManager(HTTPRepository repository) {
		this.repository = repository;
	}

	private static HTTPRepository initRepository(GdbConnectionConfig config, CloseableHttpClient httpClient) {
		String address = config.getServerUrl();
		String repositoryId = config.getRepositoryId();
		GraphDBSinkConfig.AuthenticationType authType = config.getAuthType();
		HTTPRepository repository = new HTTPRepository(address, repositoryId);
		// Basic AUTH credentials are handled when creating the repository, while mTLS auth is handled when creating the HTTP Client
		if (authType == GraphDBSinkConfig.AuthenticationType.BASIC) {
			if (LOG.isTraceEnabled()) {
				LOG.trace("Initializing repository connection with user {}", config.getUsername());
			}
			repository.setUsernameAndPassword(config.getUsername(), config.getPassword().value());
		}
		repository.setHttpClient(httpClient);
		return repository;
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
