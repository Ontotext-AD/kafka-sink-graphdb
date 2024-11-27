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

import com.ontotext.kafka.util.ValidateGraphDBConnection;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * {@link SinkConnector} implementation for streaming messages containing RDF data to GraphDB repositories
 * asynchronously
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkConnector extends SinkConnector {

	private static final Logger LOG = LoggerFactory.getLogger(GraphDBSinkConnector.class);
	private Map<String, String> properties;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		LOG.info("Starting the GraphDB SINK Connector ... ");
		this.properties = properties;

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
	}

	@Override
	public ConfigDef config() {
		return GraphDBSinkConfig.CONFIG_DEFINITION;
	}

	@Override
	public Config validate(final Map<String, String> connectorConfigs) {
		var config = super.validate(connectorConfigs);
		if (config.configValues().stream().anyMatch(cv -> !cv.errorMessages().isEmpty())) {
			return config;
		}
		return ValidateGraphDBConnection.validateGraphDBConnection(config);
	}
}
