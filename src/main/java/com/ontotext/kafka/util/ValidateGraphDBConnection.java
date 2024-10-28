package com.ontotext.kafka.util;

import static com.ontotext.kafka.GraphDBSinkConfig.AUTH_BASIC_PASS;
import static com.ontotext.kafka.GraphDBSinkConfig.AUTH_BASIC_USER;
import static com.ontotext.kafka.GraphDBSinkConfig.AUTH_TYPE;
import static com.ontotext.kafka.GraphDBSinkConfig.REPOSITORY;
import static com.ontotext.kafka.GraphDBSinkConfig.SERVER_IRI;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ontotext.kafka.GraphDBSinkConfig;

public class ValidateGraphDBConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(ValidateGraphDBConnection.class);

	public static Config validateGraphDBConnection(Config validatedConnectorConfigs) {
		List<ConfigValue> confValues = validatedConnectorConfigs.configValues();

		int serverIriId = getConfigIdByName(confValues, SERVER_IRI);
		ConfigValue serverIri = confValues.get(serverIriId);

		try {
			validateGraphDBVersion(serverIri);
		} catch (ConfigException e) {
			serverIri.addErrorMessage(e.getMessage());
			confValues.set(serverIriId, serverIri);
			return validatedConnectorConfigs;
		}

		int repositoryId = getConfigIdByName(confValues, REPOSITORY);
		ConfigValue repository = confValues.get(repositoryId);
		int authTypeId = getConfigIdByName(confValues, AUTH_TYPE);
		ConfigValue authType = confValues.get(authTypeId);

		HTTPRepository testRepository = new HTTPRepository((String) serverIri.value(), (String) repository.value());
		try {
			validateGraphDBAuthAndRepo(confValues, testRepository, authType);
		} catch (ConfigException e) {
			LOGGER.error("Could not connect to repository", e);
			authType.addErrorMessage(e.getMessage());
			confValues.set(authTypeId, authType);
		} catch (RepositoryException e) {
			LOGGER.error("Could not connect to repository", e);
			repository.addErrorMessage(new ConfigException(REPOSITORY, repository.value(),
				e.getMessage() + ": Invalid repository").getMessage());
			confValues.set(repositoryId, repository);
		}
		return validatedConnectorConfigs;
	}

	private static int getConfigIdByName(final List<ConfigValue> config, final String name) {
		return IntStream.range(0, config.size())
			       .filter(i -> name.equals(config.get(i).name()))
			       .findFirst()
			       .orElse(-1);
	}

	private static ConfigValue getConfigByName(final List<ConfigValue> config, final String name) {

		return config.stream().filter(cv -> cv.name().equals(name)).findFirst().orElse(null);
	}

	private static void validateGraphDBVersion(ConfigValue serverIri) {
		try {
			URL versionUrl;
			String version;
			if (serverIri.value().toString().endsWith("/")) {
				versionUrl = new URL(serverIri.value() + "rest/info/version");
			} else {
				versionUrl = new URL(serverIri.value() + "/rest/info/version");
			}
			try {
				version = new JSONObject(IOUtils.toString(HttpClientBuilder.create()
					                                                 .build()
					                                                 .execute(new HttpGet(versionUrl.toString()))
					                                                 .getEntity()
					                                                 .getContent(), StandardCharsets.UTF_8)).getString("productVersion");
			} catch (JSONException e) {
				throw new ConfigException(SERVER_IRI, serverIri.value(),
					"No GraphDB running on the provided GraphDB server URL");
			}
			String[] versionSplits = version.split("[.\\-]");
			if (Integer.parseInt(versionSplits[0]) < 10 && Integer.parseInt(versionSplits[1]) < 10) {
				throw new ConfigException(SERVER_IRI, serverIri.value(),
					"Kafka sink is supported on GraphDB 9.10 or newer. Please update your GraphDB");

			}
		} catch (IOException e) {
			throw new ConfigException(SERVER_IRI, serverIri.value(),
				"No GraphDB running on the provided GraphDB server URL");
		}
	}

	private static void validateGraphDBAuthAndRepo(List<ConfigValue> confValues, HTTPRepository testRepo,
		ConfigValue authType) {
		switch (GraphDBSinkConfig.AuthenticationType.of((String) authType.value())) {
			case NONE:
				break;
			case BASIC:
				testRepo.setUsernameAndPassword(
					(String) getConfigByName(confValues, AUTH_BASIC_USER).value(),
					((Password) getConfigByName(confValues, AUTH_BASIC_PASS).value()).value());
				break;
			case CUSTOM:
			default:
				throw new ConfigException(AUTH_TYPE, authType.value(), "Not supported");
		}

		try (RepositoryConnection connection = testRepo.getConnection()) {
			connection.begin();
			connection.rollback();
		} catch (RepositoryException e) {
			if (e instanceof UnauthorizedException) {
				throw new ConfigException("Invalid credentials" + e.getMessage());
			}
			throw e;
		}
	}
}
