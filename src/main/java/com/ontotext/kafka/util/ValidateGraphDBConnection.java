package com.ontotext.kafka.util;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.stream.IntStream;

import static com.ontotext.kafka.GraphDBSinkConfig.*;

public class ValidateGraphDBConnection {

	private static final Logger LOG = LoggerFactory.getLogger(ValidateGraphDBConnection.class);

	public static Config validateGraphDBConnection(Config validatedConnectorConfigs) {
		ArrayList<ConfigValue> confValues = (ArrayList<ConfigValue>) validatedConnectorConfigs.configValues();

		int serverIriId = getConfigIdByName(confValues, SERVER_URL);
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
			authType.addErrorMessage(e.getMessage());
			confValues.set(authTypeId, authType);
		} catch (RepositoryException e) {
			repository.addErrorMessage(new ConfigException(REPOSITORY, repository.value(),
				e.getMessage() + ": Invalid repository").getMessage());
			confValues.set(repositoryId, repository);
		}

		return validatedConnectorConfigs;
	}

	private static int getConfigIdByName(final ArrayList<ConfigValue> config, final String name) {

		return IntStream.range(0, config.size())
			.filter(i -> name.equals(config.get(i).name()))
			.findFirst()
			.orElse(-1);
	}

	private static ConfigValue getConfigByName(final ArrayList<ConfigValue> config, final String name) {

		return config.stream().filter(cv -> cv.name().equals(name)).findFirst().orElse(null);
	}

	private static void validateGraphDBVersion(ConfigValue serverIri) {
		try {
			LOG.trace("Validating GraphDB version");
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
				LOG.trace("Using GraphDB version {}", version);
			} catch (JSONException e) {
				LOG.error("Caught JSON exception while validating GraphDB version", e);
				throw new ConfigException(SERVER_URL, serverIri.value(),
					"No GraphDB running on the provided GraphDB server URL");
			}
			String[] versionSplits = version.split("[.\\-]");
			if (Integer.parseInt(versionSplits[0]) < 10 && Integer.parseInt(versionSplits[1]) < 10) {
				throw new ConfigException(SERVER_URL, serverIri.value(),
					"Kafka sink is supported on GraphDB 9.10 or newer. Please update your GraphDB");

			}
		} catch (IOException e) {
			LOG.error("Caught I/O exception while validating GraphDB version", e);
			throw new ConfigException(SERVER_URL, serverIri.value(),
				"No GraphDB running on the provided GraphDB server URL");
		}
	}

	private static void validateGraphDBAuthAndRepo(ArrayList<ConfigValue> confValues, HTTPRepository testRepo,
												   ConfigValue authType) {
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
					(String) getConfigByName(confValues, AUTH_BASIC_USER).value(),
					((Password) getConfigByName(confValues, AUTH_BASIC_PASS).value()).value());
				break;
			case CUSTOM:
			default:
				throw new ConfigException(AUTH_TYPE, authType.value(), "Not supported");
		}

		try (RepositoryConnection connection = testRepo.getConnection()) {
			LOG.trace("Starting repository connection test");
			connection.begin();
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
