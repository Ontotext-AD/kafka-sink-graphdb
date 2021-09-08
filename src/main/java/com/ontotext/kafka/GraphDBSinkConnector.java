/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ontotext.kafka;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ontotext.kafka.service.GraphDBService;
import com.ontotext.kafka.util.PropertiesUtil;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.json.JSONObject;

/**
 * {@link SinkConnector} implementation for streaming messages containing RDF data to GraphDB repositories
 * asynchronously through {@link GraphDBService} .
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkConnector extends SinkConnector {

	private Map<String, String> properties;

	@Override
	public String version() {
		return PropertiesUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		this.properties = properties;
		GraphDBService.connectorService().initialize(properties);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return GraphDBSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(properties);
		}
		return taskConfigs;
	}

	@Override
	public void stop() {
		GraphDBService.connectorService().shutDown();
	}

	@Override
	public ConfigDef config() {
		return GraphDBSinkConfig.createConfig();
	}

	@Override
	public Config validate(final Map<String, String> connectorConfigs) {
		var config = super.validate(connectorConfigs);
		try {
			GraphDBSinkConfig sinkConfig = new GraphDBSinkConfig(connectorConfigs);
		} catch (Exception e) {
			return config;
		}
		validateGraphDBConnection(connectorConfigs);
		return config;
	}

	private void validateGraphDBConnection(Map<String, String> connectorConfigs) {
		String serverIri = connectorConfigs.get(GraphDBSinkConfig.SERVER_IRI);
		String repositoryId = connectorConfigs.get(GraphDBSinkConfig.REPOSITORY);
		//NOTE: when the user doesn't have write permissions it says invalid repo
		try {
			URL versionUrl = new URL(serverIri + "rest/info/version");
			String version = new JSONObject(IOUtils.toString(versionUrl, Charset.defaultCharset())).getString("productVersion");

			int major = Integer.parseInt(version.split("\\.")[0]);
			if (major < 10) {
				int minor = Integer.parseInt(version.split("\\.")[1]);
				if (major == 9 && minor < 9) {
					throw new ConnectException("Kafka sink is supported on GraphDB 9.10 or higher. Please update your GraphDB");
				}
			}
		} catch (IOException e) {
			throw new ConnectException("No GraphDB running on the provided GraphDB server iri");
		}

		var repository = new HTTPRepository(serverIri, repositoryId);
		switch (GraphDBSinkConfig.AuthenticationType.of(connectorConfigs.get(GraphDBSinkConfig.AUTH_TYPE))) {
			case NONE:
				break;
			case BASIC:
				repository.setUsernameAndPassword(
						connectorConfigs.get(GraphDBSinkConfig.AUTH_BASIC_USER),
						connectorConfigs.get(GraphDBSinkConfig.AUTH_BASIC_PASS));
				break;
			case CUSTOM:
			default:
				throw new UnsupportedOperationException(connectorConfigs.get(GraphDBSinkConfig.AUTH_TYPE) + " not supported");
		}
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			connection.rollback();
		} catch (RepositoryException e) {
			if (e instanceof UnauthorizedException) {
				throw new ConnectException("Invalid credentials");
			}
			throw new ConnectException("Invalid repository");
		}
	}
}