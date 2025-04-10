package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.kafka.common.config.ConfigException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ontotext.kafka.GraphDBSinkConfig.AUTH_TYPE;
import static com.ontotext.kafka.GraphDBSinkConfig.SERVER_URL;
import static com.ontotext.kafka.tls.HttpsClientManager.createHttpClient;

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
			this.repository = initRepository(config,
				createHttpClient(config.getServerUrl(), config.getTlsThumbprint(), config.isHostnameVerificationEnabled()));
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
		switch (authType) {
			case NONE:
				return repository;
			case BASIC:
				if (LOG.isTraceEnabled()) {
					LOG.trace("Initializing repository connection with user {}", config.getUsername());
				}
				repository.setUsernameAndPassword(config.getUsername(), config.getPassword().value());
				repository.setHttpClient(httpClient);
				return repository;
			default: // Any other types which are valid, as per definition, but are not implemented yet
				throw new ConfigException(AUTH_TYPE, authType, "Authentication type is not supported or is invalid");
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
