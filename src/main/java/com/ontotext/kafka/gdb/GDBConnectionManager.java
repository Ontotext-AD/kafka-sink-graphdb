package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.kafka.common.config.ConfigException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.Charset;

import static com.ontotext.kafka.GraphDBSinkConfig.SERVER_URL;
import static com.ontotext.kafka.tls.HttpClientManager.createHttpClient;

public class GDBConnectionManager {

	private static final Logger log = LoggerFactory.getLogger(GDBConnectionManager.class);
	private final HTTPRepository repository;
	private final GdbConnectionConfig config;
	private final HttpClient httpClient;

	/**
	 * Creates the manager given the entire connector configuration
	 *
	 * @param config - the configuration of the Kafka Sink Connector
	 * @throws org.apache.kafka.common.config.ConfigException if a configuration property was not validated
	 */
	public GDBConnectionManager(GdbConnectionConfig config) {
		try {
			this.config = config;
			this.httpClient = createHttpClient(config);
			this.repository = initRepository(config, httpClient);
		} catch (ConfigException e) {
			throw e;
		} catch (Exception e) {
			throw new ConfigException(SERVER_URL, config.getServerUrl(), e.getMessage());
		}

	}

	/**
	 * For testing only
	 *
	 * @param repository the mock/test repository
	 */
	GDBConnectionManager(HTTPRepository repository, GdbConnectionConfig config, HttpClient httpClient) {
		this.repository = repository;
		this.httpClient = httpClient;
		this.config = config;
	}

	private static HTTPRepository initRepository(GdbConnectionConfig config, HttpClient httpClient) {
		String address = config.getServerUrl();
		String repositoryId = config.getRepositoryId();
		GraphDBSinkConfig.AuthenticationType authType = config.getAuthType();
		HTTPRepository repository = new HTTPRepository(address, repositoryId);
		// Basic AUTH credentials are handled when creating the repository, while mTLS auth is handled when creating the HTTP Client
		if (authType == GraphDBSinkConfig.AuthenticationType.BASIC) {
			if (log.isTraceEnabled()) {
				log.trace("Initializing repository connection with user {}", config.getUsername());
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

	public void validateGDBVersion() {
		String serverUrl = config.getServerUrl();
		try {
			log.trace("Validating GraphDB version");
			URL versionUrl = new URL(String.format("%s/rest/info/version", serverUrl));
			String version;
			HttpResponse response = httpClient.execute(new HttpGet(versionUrl.toString()));
			int statusCode = response.getStatusLine().getStatusCode();
			String content = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
			switch (statusCode) {
				case 200:
					try {
						version = new JSONObject(content).getString("productVersion");
						break;
					} catch (JSONException e) {
						log.error("Could not parse version from response", e);
						throw new GdbConnectionConfigException(SERVER_URL, serverUrl, "Could not parse version from response. Most probably no GraphDB running on the provided server URL");
					}
				case 401:
					log.error("Unauthorized - {}", content);
					throw new GdbConnectionConfigException(SERVER_URL, serverUrl, "Authentication failed");
				case 403:
					log.error("Forbidden - {}", content);
					throw new GdbConnectionConfigException(SERVER_URL, serverUrl, "Access denied");
				case 404:
					throw new GdbConnectionConfigException(SERVER_URL, serverUrl, "Invalid URL - No GraphDB running on the provided server URL");
				default:
					throw new GdbConnectionConfigException(SERVER_URL, serverUrl, String.format("Error - got %d response code with %s content", statusCode, content));
			}

			String[] versionSplits = version.split("[.\\-]");
			int major = Integer.parseInt(versionSplits[0]);
			int minor = Integer.parseInt(versionSplits[1]);
			boolean versionSupported = false;
			switch (major) {
				case 9: // 9.11+
					if (minor >= 11) {
						versionSupported = true;
					}
					break;
				case 10: // 10.8+
					if (minor > 8) {
						versionSupported = true;
					}
					break;
				case 11: // 11+
					versionSupported = true;
					break;
			}
			if (!versionSupported) {
				throw new GdbConnectionConfigException(SERVER_URL, serverUrl,
					"Kafka sink is supported on GraphDB versions 10.8+, 11+ and 9.11+. Please update your GraphDB");
			}
			log.info("Using GraphDB version {}", version);
		} catch (Exception e) {
			if (e instanceof GdbConnectionConfigException) {
				throw (GdbConnectionConfigException) e;
			}
			log.error("Caught exception while validating GraphDB version", e);
			throw new GdbConnectionConfigException(SERVER_URL, serverUrl, "Error while validating GraphDB version");
		}
	}


}
