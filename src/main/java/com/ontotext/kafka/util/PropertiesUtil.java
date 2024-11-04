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

package com.ontotext.kafka.util;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {

	private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);
	private static String version = "0.0.1";
	private static Properties properties;

	private PropertiesUtil() {
	}

	static {
		try {
			properties = new Properties();
			properties.load(PropertiesUtil.class.getResourceAsStream("/graphdb-kafka-version.properties"));
			properties.load(PropertiesUtil.class.getResourceAsStream("/graphdb-kafka-sink.properties"));
			version = properties.getProperty("graphdb.version", version).trim();
			LOG.info("GraphDB version: {}", version);
		} catch (Exception e) {
			LOG.warn("error while loading version:", e);
		}
	}

	public static String getVersion() {
		return version;
	}

	public static String getProperty(String key) {
		return properties.getProperty(key);
	}

	public static ToleranceType getTolerance(Map<String, ?> properties) {
		String tolerance = (String) properties.get(ConnectorConfig.ERRORS_TOLERANCE_CONFIG);
		if (tolerance == null || "none".equalsIgnoreCase(tolerance)) {
			return ToleranceType.NONE;
		} else if ("all".equalsIgnoreCase(tolerance)) {
			return ToleranceType.ALL;
		} else
			throw new DataException("error: Tolerance can be \"none\" or \"all\". Not supported for - "
				+ tolerance);
	}
}
