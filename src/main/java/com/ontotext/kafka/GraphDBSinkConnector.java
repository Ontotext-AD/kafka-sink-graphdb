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

import com.ontotext.kafka.util.GraphDBConnectionValidator;
import com.ontotext.kafka.util.TransformationsValidator;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.ontotext.kafka.GraphDBSinkConfig.LOGGER_TYPE;
import static com.ontotext.kafka.GraphDBSinkConfig.LOG_LEVEL_OVERRIDE;
import static com.ontotext.kafka.logging.LoggerFactory.LOGGER_TYPE_PROPERTY;
import static com.ontotext.kafka.logging.LoggerFactory.LOG_LEVEL_PROPERTY;
import static com.ontotext.kafka.processor.SinkProcessorManager.startNewProcessor;
import static com.ontotext.kafka.processor.SinkProcessorManager.stopProcessor;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;

/**
 * {@link SinkConnector} implementation for streaming messages containing RDF data to GraphDB repositories
 * asynchronously
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkConnector extends SinkConnector {

	private static final Logger log = LoggerFactory.getLogger(GraphDBSinkConnector.class);

	private GraphDBSinkConfig config;
	private Map<String, String> properties;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		this.config = new GraphDBSinkConfig(properties);
		this.properties = properties;
		log.info("[{}] Starting the GraphDB SINK Connector", config.getConnectorName());
		startNewProcessor(config);

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
		log.trace("[{}] Shutting down processor", config.getConnectorName());
		stopProcessor(config.getConnectorName());
	}

	@Override
	public ConfigDef config() {
		return GraphDBSinkConfig.CONFIG_DEFINITION;
	}

	@Override
	public Config validate(final Map<String, String> connectorConfigs) {
		Config config = super.validate(connectorConfigs);
		configureLogging(connectorConfigs);

		new TransformationsValidator().validate(config, connectorConfigs);


		if (config.configValues().stream().anyMatch(cv -> !cv.errorMessages().isEmpty())) {
			return config;
		}
		new GraphDBConnectionValidator().validate(config, connectorConfigs);
		return config;
	}

	/**
	 * Try to determine if the connector is deployed in MSK Cluster. Use bootstrap servers to identify if MSK servers are used.
	 * Configure log level
	 *
	 * @param connectorConfigs
	 */
	private void configureLogging(Map<String, String> connectorConfigs) {
		String connectorName = connectorConfigs.get(NAME_CONFIG);
		String logLevel = connectorConfigs.get(LOG_LEVEL_OVERRIDE);
		Level level = Level.toLevel(logLevel, Level.INFO);
		log.debug("[{}] Configuring log level = {}", connectorName, logLevel);
		System.setProperty(LOG_LEVEL_PROPERTY, level.toString());

		com.ontotext.kafka.logging.LoggerFactory.LoggerType loggerType = com.ontotext.kafka.logging.LoggerFactory.LoggerType.getLoggerType(connectorConfigs.get(LOGGER_TYPE));
		log.info("[{}] Configuring logger type = {}", connectorName, loggerType);
		System.setProperty(LOGGER_TYPE_PROPERTY, loggerType.toString());

	}

}
