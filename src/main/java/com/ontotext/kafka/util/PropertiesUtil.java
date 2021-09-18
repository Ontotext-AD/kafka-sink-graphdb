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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PropertiesUtil {

	public static final String ERRORS_TOLERANCE = "errors.tolerance";
	public static final String CONNECTION_RETRY_DEFERRED_TIME = "connection.retry.deferred.time";
	public static final long DEFAULT_CONNECTION_RETRY_DEFERRED_TIME = 100L;
	public static final String CONNECTION_NUMBER_OF_RETRIES = "connection.retry.number.of.times";
	public static final int DEFAULT_CONNECTION_NUMBER_OF_RETRIES = 10;
	public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

	private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

	private static String version = "unknown";

	private static Properties properties;

	private PropertiesUtil() {
	}

	static {
		try {
			properties = new Properties();
			properties.load(PropertiesUtil.class.getResourceAsStream("/graphdb-kafka-version.properties"));
			properties.load(PropertiesUtil.class.getResourceAsStream("/graphdb-kafka-sink.properties"));
			version = properties.getProperty("graphdb.version", version).trim();
		} catch (Exception e) {
			LOGGER.warn("error while loading version:", e);
		}
	}

	public static String getVersion() {
		return version;
	}

	public static String getProperty(String key) {
		return properties.getProperty(key);
	}

	public static long getFromPropertyOrDefault(String propertyName, Long defaultValue) {
		String propertyValue = getProperty(propertyName);
		try {
			return Long.parseLong(propertyValue);

		} catch (NumberFormatException e) {
			LOGGER.error("Property: " + propertyName +
					" has incorrect value :" + propertyValue +
					". Using default: " + defaultValue);
			return defaultValue;
		}

	}

	public static int getFromPropertyOrDefault(String propertyName, Integer defaultValue) {
		String propertyValue = getProperty(propertyName);
		try {
			return Integer.parseInt(propertyValue);

		} catch (NumberFormatException e) {
			LOGGER.error("Property: " + propertyName +
					" has incorrect value :" + propertyValue +
					". Using default: " + defaultValue);
			return defaultValue;
		}
	}

	public static String getFromPropertyOrDefault(String propertyName, String defaultValue) {
		String propertyValue = getProperty(propertyName);
		return propertyValue != null ? propertyValue : defaultValue;
	}
}
