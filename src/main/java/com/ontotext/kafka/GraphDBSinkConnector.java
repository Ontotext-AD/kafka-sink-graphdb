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

import com.ontotext.kafka.transformation.RdfTransformation;
import com.ontotext.kafka.util.GraphDBConnectionValidator;
import com.ontotext.kafka.util.VersionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.ontotext.kafka.processor.SinkProcessorManager.startNewProcessor;
import static com.ontotext.kafka.processor.SinkProcessorManager.stopProcessor;

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
		log.info("Starting the GraphDB SINK Connector {} ... ", config.getConnectorName());
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
		log.trace("Shutting down processor");
		stopProcessor(config.getConnectorName());
	}

	@Override
	public ConfigDef config() {
		return GraphDBSinkConfig.CONFIG_DEFINITION;
	}

	@Override
	public Config validate(final Map<String, String> connectorConfigs) {
		Config config = super.validate(connectorConfigs);
		// Validate transformations
		String transformationsNames = connectorConfigs.get("transforms");
		if (StringUtils.isNotEmpty(transformationsNames)) {
			log.debug("Got transformations {}", transformationsNames);
			for (String transformation : transformationsNames.split(",")) {
				if (transformation.isEmpty()) {
					log.warn("Invalid transformation name: {}", transformation);
					break;
				}
				String transformationClass = getTransformationClass(transformation.trim(), connectorConfigs);
				try {
					if (transformationClass == null) {
						log.warn("Did not find transformation class for transformation {}.", transformation);
						break;
					}
					Class<?> clazz = Class.forName(transformationClass);
					if (RdfTransformation.class.isAssignableFrom(clazz)) {
						RdfTransformation transform = (RdfTransformation) clazz
							.getDeclaredConstructor()
							.newInstance();
						transform.validateConfig(transformation, connectorConfigs);
						log.debug("Transformation {} validated", transformation);

					}
				} catch (ClassNotFoundException e) {
					log.warn("Transformation class not found: {}", transformationClass, e);
				} catch (ReflectiveOperationException e) {
					log.warn("Transformation class cannot be initialized: {}", transformationClass, e);
				} catch (ConfigException e) {
					log.error("Transformation class did not validate - {}", transformationClass, e);
					ConfigValue transformationValidationError = new ConfigValue(
						String.format("transforms.%s", transformation));
					transformationValidationError.addErrorMessage("Invalid transformation configurations.");
					config.configValues().add(transformationValidationError);
				}
			}
		}
		if (config.configValues().stream().anyMatch(cv -> !cv.errorMessages().isEmpty())) {
			return config;
		}
		return GraphDBConnectionValidator.validateGraphDBConnection(config);
	}

	private String getTransformationClass(String transformationName, final Map<String, String> connectorConfigs) {
		if (StringUtils.isNotEmpty(transformationName)) {
			String transformTypeKey = String.format("transforms.%s.type", transformationName);
			String transformType = connectorConfigs.get(transformTypeKey);
			if (StringUtils.isEmpty(transformType)) {
				log.warn("Missing or empty type for transformation '{}'. Expected key: {}", transformationName,
					transformTypeKey);
				return null;
			}
			return transformType;
		}
		return null;
	}
}
