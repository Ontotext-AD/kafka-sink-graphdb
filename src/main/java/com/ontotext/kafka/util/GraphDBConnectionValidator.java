package com.ontotext.kafka.util;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.rdf.repository.RepositoryManager;
import com.ontotext.kafka.tls.HttpsClientManager;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ontotext.kafka.GraphDBSinkConfig.*;
import static com.ontotext.kafka.GraphDBSinkConfig.AuthenticationType.NONE;

public final class GraphDBConnectionValidator {
	private static final Logger LOG = LoggerFactory.getLogger(GraphDBConnectionValidator.class);


	private GraphDBConnectionValidator() {
		throw new IllegalStateException("Utility class");
	}


	public static Config validateGraphDBConnection(Config validatedConnectorConfigs) {

		Map<String, ConfigValue> configValues = validatedConnectorConfigs.configValues().stream()
			.collect(Collectors.toMap(ConfigValue::name, Function.identity()));

		ConfigValue serverIri = configValues.get(SERVER_URL);
		String serverUrl = (String) serverIri.value();
		String thumbprint = (String) configValues.get(TLS_THUMBPRINT).value();
		try {
			validateAndCreateHttpClient(serverUrl, thumbprint);
		} catch (ConfigException e) {
			serverIri.addErrorMessage(e.getMessage());
			return validatedConnectorConfigs;
		}

		try {
			validateGraphDBVersion(serverUrl);
		} catch (ConfigException e) {
			serverIri.addErrorMessage(e.getMessage());
			return validatedConnectorConfigs;
		}

		ConfigValue repository = configValues.get(REPOSITORY);
		ConfigValue authType = configValues.get(AUTH_TYPE);
		if (authType == null) {
			LOG.warn("No auth type for repository connection provided. Assuming {}", NONE);
			authType = new ConfigValue(AUTH_TYPE, NONE, Collections.emptyList(), Collections.emptyList());
		}
		try {
			HTTPRepository testRepository = RepositoryManager.createHttpRepository(serverUrl, thumbprint, (String) repository.value());
			doValidate(configValues, testRepository, authType);
		} catch (ConfigException e) {
			authType.addErrorMessage(e.getMessage());
		} catch (RepositoryException e) {
			repository.addErrorMessage(new ConfigException(REPOSITORY, repository.value(),
				e.getMessage() + ": Invalid repository").getMessage());
		} catch (IOException e) {
			serverIri.addErrorMessage(e.getMessage());
		}

		return validatedConnectorConfigs;
	}

	private static void validateAndCreateHttpClient(String serverUrl, String tlsThumbprint) {
		if (!HttpsClientManager.isUrlHttps(serverUrl)) {
			// Ignore thumbprint validation since we are not working with TLS
			return;
		}
		if (StringUtils.isEmpty(tlsThumbprint)) {
			throw new ConfigException("Using TLS but no TLS thumbprint provided");
		}

		try {
			if (HttpsClientManager.getCertificate(serverUrl, tlsThumbprint) == null) {
				throw new ConfigException(String.format("Server %s did not return a certificate that has thumbprint matching %s", serverUrl, tlsThumbprint));
			}
		} catch (Exception e) {
			LOG.error("Caught exception while initializing TLS context", e);
			throw new ConfigException(e.getMessage());
		}
	}

	private static void validateTemplate(RepositoryConnection connection, String templateId) {
		String templateContentQ = "select ?template {\n <%s> <http://www.ontotext.com/sparql/template> ?template\n}";
		try (TupleQueryResult templates = connection.prepareTupleQuery(String.format(templateContentQ, templateId)).evaluate()) {
			String template = null;
			if (templates.hasNext()) {
				// Only interested in first result
				template = templates.next().getValue("template").stringValue();
			}
			if (StringUtils.isEmpty(template)) {
				throw new ConfigException("Did not find template with ID {}", templateId);
			}
			LOG.info("Found template {}", template);
		}
	}

	private static void validateGraphDBVersion(String serverUrl) {
		try {
			LOG.trace("Validating GraphDB version");
			URL versionUrl;
			String version;
			if (serverUrl.toString().endsWith("/")) {
				versionUrl = new URL(serverUrl + "rest/info/version");
			} else {
				versionUrl = new URL(serverUrl + "/rest/info/version");
			}
			try {
				version = new JSONObject(IOUtils.toString(HttpClientBuilder.create()
					.setSSLContext(SSLContext.getDefault())
					.setSSLHostnameVerifier((hostname, session) -> true)
					.build()
					.execute(new HttpGet(versionUrl.toString()))
					.getEntity()
					.getContent(), StandardCharsets.UTF_8)).getString("productVersion");
				LOG.trace("Using GraphDB version {}", version);
			} catch (JSONException e) {
				LOG.error("Caught JSON exception while validating GraphDB version", e);
				throw new ConfigException(SERVER_URL, serverUrl,
					"No GraphDB running on the provided GraphDB server URL");
			} catch (NoSuchAlgorithmException e) {
				throw new ConfigException(SERVER_URL, serverUrl,
					"No GraphDB running on the provided GraphDB server URL");
			}
			String[] versionSplits = version.split("[.\\-]");
			if (Integer.parseInt(versionSplits[0]) < 10 && Integer.parseInt(versionSplits[1]) < 10) {
				throw new ConfigException(SERVER_URL, serverUrl,
					"Kafka sink is supported on GraphDB 9.10 or newer. Please update your GraphDB");

			}
		} catch (IOException e) {
			LOG.error("Caught I/O exception while validating GraphDB version", e);
			throw new ConfigException(SERVER_URL, serverUrl,
				"No GraphDB running on the provided GraphDB server URL");
		}
	}

	private static void doValidate(Map<String, ConfigValue> configValues, HTTPRepository testRepo, ConfigValue authType) {
		LOG.trace("Validating GraphDB authentication and repository");
		String authTypeString = (String) authType.value();
		if (StringUtils.isBlank(authTypeString)) {
			throw new ConfigException(AUTH_TYPE, "Not provided");
		}
		AuthenticationType type = GraphDBSinkConfig.AuthenticationType.valueOf(authTypeString.toUpperCase());

		switch (type) {
			case NONE:
				break;
			case BASIC:
				testRepo.setUsernameAndPassword(
					(String) configValues.get(AUTH_BASIC_USER).value(),
					((Password) configValues.get(AUTH_BASIC_PASS).value()).value());
				break;
			case CUSTOM:
			default:
				throw new ConfigException(AUTH_TYPE, authType.value(), "Not supported");
		}

		ConfigValue templateIdValue = configValues.get(TEMPLATE_ID);
		try (RepositoryConnection connection = testRepo.getConnection()) {
			LOG.trace("Starting repository connection test");
			connection.begin();
			if (templateIdValue != null && StringUtils.isNotEmpty((String) templateIdValue.value())) {
				String templateId = (String) templateIdValue.value();
				LOG.info("Querying template ID {} from repository {}", templateId, testRepo);
				validateTemplate(connection, templateId);
			}
			connection.rollback();
			LOG.trace("Rolled back repository connection test");
		} catch (RepositoryException e) {
			if (e instanceof UnauthorizedException) {
				throw new ConfigException("Invalid credentials" + e.getMessage());
			}
			throw e;
		}
	}
}
